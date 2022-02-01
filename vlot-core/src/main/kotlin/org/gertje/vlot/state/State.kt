package org.gertje.vlot.state

import kotlinx.coroutines.sync.Mutex
import org.gertje.vlot.PeerId
import org.gertje.vlot.Role
import org.gertje.vlot.Role.Candidate
import org.gertje.vlot.Role.Follower
import org.gertje.vlot.Role.Leader
import org.gertje.vlot.Term
import java.lang.Long.max

class State<T>(val clusterSize: Int, val log: Log<T>) {
    val mutex = Mutex()

    var role: Role = Follower
        private set
    var currentTerm: Term = 0
        private set
    var votedFor: PeerId = -1
        private set
    var commitIndex: Long = -1
    var lastApplied: Long = -1
    val nextIndex: LongArray = longArrayOf(0, 0, 0)
    val matchIndex: LongArray = longArrayOf(0, 0, 0)
    var leaderId: PeerId = -1
        private set

    fun changeToFollower(term: Term) {
        currentTerm = term
        role = Follower
        votedFor = -1
    }

    fun changeToLeader() {
        role = Leader
        votedFor = -1
        nextIndex.indices.forEach { index ->
            nextIndex[index] = log.lastLogIndex + 1
            matchIndex[index] = -1
        }
    }

    fun changeToCandidate(me: PeerId) {
        role = Candidate
        currentTerm++
        votedFor = me
    }

    fun updateClientIndexes(peerId: PeerId, newMatchIndex: Long) {
        nextIndex[peerId] = max(nextIndex[peerId], newMatchIndex + 1)
        matchIndex[peerId] = max(matchIndex[peerId], newMatchIndex)
    }

    fun decreaseNextIndex(peerId: PeerId) {
        if (nextIndex[peerId] > 0) {
            nextIndex[peerId]--
        }
    }

    fun castVote(candidateId: PeerId) {
        votedFor = candidateId
    }
}

interface Log<T> {
    suspend fun get(index: Long): LogItem<T>?
    suspend fun set(index: Long, item: T, term: Term)
    suspend fun append(item: T, term: Term): Long
    suspend fun prune(index: Long)
    suspend fun slice(longRange: LongRange): List<LogItem<T>>

    val lastLogIndex: Long
    val lastLogTerm: Long
}

data class LogItem<T>(val value: T, val term: Term)

fun <T> LogItem<T>?.getTerm(): Term = this?.term ?: -1
