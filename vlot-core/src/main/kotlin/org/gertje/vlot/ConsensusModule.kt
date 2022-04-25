package org.gertje.vlot

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.takeWhile
import mu.KotlinLogging
import org.gertje.vlot.Role.Leader
import org.gertje.vlot.election.ElectionResult.*
import org.gertje.vlot.election.ElectionVoteCounter
import org.gertje.vlot.state.State
import org.gertje.vlot.state.getTerm
import java.io.Closeable
import java.lang.Long.min

private val logger = KotlinLogging.logger {}

/**
 * The [ConsensusModule] contains the Raft algorithm. It has a number of methods that can be called from a server layer
 * and a client is injected to call these methods on the other Raft nodes.
 */

typealias PeerId = Int
typealias Term = Long

class ConsensusModule<T>(
    private val id: PeerId,
    peerIds: Set<PeerId>,
    private val client: RaftClient<T>,
    private val heartBeatTicker: ResettableTicker,
    private val electionTicker: ResettableTicker,
    private val state: State<T>,
    private val stateWriter: StateWriter<T>,
    private val stateMachine: StateMachine<T>,
): Closeable {

    init {
        electionTicker.start()
    }

    private val someFlow = MutableSharedFlow<Unit>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_LATEST,
    )
    private val job = CoroutineScope(Dispatchers.Default).launch {
        someFlow.collect {
            updateFollowers()
        }
    }

    private val appliedSharedFlow = MutableStateFlow(LastAppliedTerm(-1, -1))

    private val otherPeerIds = peerIds.filter { it != id }.toSet()
    private val clusterSize: Int = peerIds.size

    suspend fun onAppendEntries(request: AppendEntriesRequest<T>): AppendEntriesResponse =
        changeState("onAppendEntries") {
            logger.info { "$id: \uD83D\uDCE5 onAppendEntries" }
            if (request.term > currentTerm) {
                logger.info { "$id: request.term > currentTerm" }
                changeStateToFollower(request.term)
            }
            logger.info { "$id: request.term = ${request.term}, currentTerm = $currentTerm" }

            if (request.term < currentTerm) {
                return@changeState AppendEntriesResponse(currentTerm, false)
            }

            electionTicker.reset()

            if (!logContainsEntry(request.prevLogIndex, request.prevLogTerm)) {
                return@changeState AppendEntriesResponse(currentTerm, false)
            }

            var logIndex = request.prevLogIndex + 1

            for (entry in request.entries) {
                when (log.get(logIndex).getTerm()) {
                    entry.term -> {
                        logger.info { "Ignoring entry $entry at logIndex: $logIndex" }
                    }
                    -1L -> {
                        logger.info { "Adding entry to log: $logIndex, $entry" }
                        log.set(logIndex, entry.value, entry.term)
                    }
                    else -> {
                        log.prune(logIndex)
                        log.set(logIndex, entry.value, entry.term)
                    }
                }
                logIndex++
            }
            if (request.leaderCommit > commitIndex) {
                commitIndex = min(request.leaderCommit, log.lastLogIndex)
            }
            leaderId = request.leaderId
            return@changeState AppendEntriesResponse(currentTerm, true)
        }

    suspend fun onRequestVote(request: RequestVoteRequest): RequestVoteResponse =
        changeState("onRequestVote") {
            logger.info { "$id: ☑️ onRequestVote" }
            if (request.term > currentTerm) {
                logger.info { "$id: request.term > currentTerm" }
                changeStateToFollower(request.term)
            }

            if (request.term < currentTerm) {
                return@changeState RequestVoteResponse(currentTerm, false)
            }

            val voteGranted = if (shouldCastVote(request)) {
                castVote(request.candidateId)
                electionTicker.reset()
                true
            } else {
                false
            }
            return@changeState RequestVoteResponse(currentTerm, voteGranted)
        }

    private suspend fun StateReader<T>.shouldCastVote(request: RequestVoteRequest): Boolean =
        (votedFor == -1 || votedFor == request.candidateId)
                && isLogUpToDate(request.lastLogIndex, request.lastLogTerm)

    suspend fun onHeartbeatTimeout() {
        logger.info { "$id: \uD83D\uDC93 onHeartbeatTimeout" }
        sendHeartBeat()
        logger.info { "$id: sent heartbeat" }
    }

    private suspend fun sendHeartBeat() {
        logger.info { "$id: readState" }
        val requests = readState {
            otherPeerIds.map { peerId ->
                val nextIndex = nextIndex[peerId]
                val prevLogIndex = nextIndex - 1
                val prevLogTerm = log.get(prevLogIndex).getTerm()
                peerId to AppendEntriesRequest<T>(currentTerm, id, prevLogIndex, prevLogTerm, emptyList(), commitIndex)
            }
        }
        logger.info { "$id: resetting heart beat ticker" }
        heartBeatTicker.reset()
        logger.info { "$id: reset heart beat ticker" }
        coroutineScope {
            for ((peerId, request) in requests) {
                launch {
                    updateFollower(peerId, request)
                }
            }
        }
    }

    suspend fun addMessage(message: T): AddMessageResponse {
        val (index, term) = changeState("addMessage") {
            if (role != Leader) {
                return WrongLeader(leaderId)
            }
            // Append the message to the log.
            val newIndex = log.append(message, currentTerm)
            newIndex to currentTerm
        }

        someFlow.emit(Unit)

        return withTimeoutOrNull(20_000) {
            appliedSharedFlow
                .takeWhile { (appliedIndex, appliedTerm) ->
                    appliedIndex < index || appliedTerm < term
                }
                .lastOrNull()
            Success
        } ?: TimeOut
    }

    private suspend fun updateFollowers() {
        val requests = readState {
            otherPeerIds.map { peerId ->
                val nextIndex = nextIndex[peerId]
                val prevLogIndex = nextIndex - 1
                val prevLogTerm = log.get(prevLogIndex).getTerm()
                val currentIndex = log.lastLogIndex
                val entries = if (currentIndex >= nextIndex) {
                    log.slice(nextIndex..currentIndex)
                } else {
                    emptyList()
                }
                    .map { item -> Entry(item.term, item.value) }

                peerId to AppendEntriesRequest(currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex)
            }
        }

        // Check if there is at least one request with non-empty entry list, note that we either send the requests to
        // all clients or we send none, because of the heart beat.
        if (requests.all { (_, request) -> request.entries.isEmpty() }) {
            return
        }

        heartBeatTicker.reset()
        logger.info { "$id: reset heart beat ticker" }
        coroutineScope {
            requests
                .map { (peerId, request) ->
                    async { updateFollower(peerId, request) }
                }
                .awaitAll()
        }
    }

    private suspend fun updateFollower(peerId: PeerId, request: AppendEntriesRequest<T>) {
        val response = client.appendEntries(peerId, request)
        changeState("updateFollower") {
            if (response.term > currentTerm) {
                changeStateToFollower(response.term)
                return@changeState
            }

            if (response.success) {
                updateClientIndexes(peerId, request.prevLogIndex + request.entries.size)
            } else {
                decreaseNextIndex(peerId)
            }
        }
    }

    suspend fun onElectionTimeout() {
        logger.info { "$id: \uD83D\uDDF3️ onElectionTimeout" }

        val request = changeState("onElectionTimeout1") {
            changeStateToCandidate()
            logger.info { "$id Create vote requests" }
            RequestVoteRequest(currentTerm, id, log.lastLogIndex, log.lastLogTerm)
        }

        logger.info { "$id Sending vote requests" }
        coroutineScope {
            val channel = sendVoteRequests(request) // Fan-out

            logger.info { "$id Channel for sending is open" }

            val voteCounter = ElectionVoteCounter(clusterSize)
            for (response in channel) {
                coroutineScope {
                    launch {
                        logger.info { "$id: new response $response" }
                        val currentTerm = readState { currentTerm }
                        if (response.term > currentTerm) {
                            changeState("onElectionTimeout2") {
                                changeStateToFollower(response.term)
                            }
                            channel.cancel()
                            cancel() // Don't call cancel from within mutex
                        }
                        logger.info { "$id: all well" }
                        val electionResult = voteCounter.count(response.voteGranted)
                        if (electionResult == UNDETERMINED) {
                            return@launch
                        }
                        logger.info { "$id: result $electionResult" }
                        if (electionResult == WON) {
                            changeState("onElectionTimeout3") {
                                changeStateToLeader()
                            }
                        }
                        channel.cancel() // break
                        cancel()
                    }
                }
            }
        }
    }

    private fun CoroutineScope.sendVoteRequests(
        request: RequestVoteRequest,
    ): ReceiveChannel<RequestVoteResponse> = produce {
        otherPeerIds.forEach { peerId ->
            launch {
                send(client.requestVote(peerId, request))
            }
            logger.info { "$id: launched for peer $peerId" }
        }
        logger.info { "$id: done for each" }
    }

    private fun StateMutator<T>.changeStateToFollower(term: Term) {
        logger.info { "$id: changing to follower" }
        changeToFollower(term)
    }

    private fun StateMutator<T>.changeStateToLeader() {
        logger.info { "$id: changing to leader" }
        changeToLeader()
    }

    private fun StateMutator<T>.changeStateToCandidate() {
        logger.info { "$id: changing to candidate" }
        changeToCandidate(id)
    }

    private suspend inline fun <V> changeState(
        name: String,
        action: StateMutator<T>.() -> V,
    ): V = with(changeState(name, state, stateWriter, stateMachine, action)) {
        if (lastAppliedTerm != null) {
            appliedSharedFlow.emit(lastAppliedTerm)
        }
        if (changedToFollower) {
            heartBeatTicker.stop()
            electionTicker.start()
        }
        if (changedToLeader) {
            electionTicker.stop()
            heartBeatTicker.start()
        }
        if (changedToCandidate) {
            heartBeatTicker.stop()
            electionTicker.start()
            electionTicker.reset()
        }
        value
    }

    private suspend inline fun <V> readState(action: StateReader<T>.() -> V): V =
        readState(state, action)

    override fun close() {
        job.cancel()
    }
}

enum class Role {
    Candidate,
    Follower,
    Leader,
}

interface StateMachine<T> {
    fun apply(value: T)
}
