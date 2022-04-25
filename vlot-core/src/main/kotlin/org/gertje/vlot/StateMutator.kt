package org.gertje.vlot

import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.gertje.vlot.state.Log
import org.gertje.vlot.state.State

private val log = KotlinLogging.logger {}

class StateMutator<T>(private val delegate: State<T>): StateReader<T>(delegate) {

    override var commitIndex: Long
        get() = delegate.commitIndex
        set(value) {
            isMutated = true
            delegate.commitIndex = value
        }

    override var lastApplied: Long
        get() = delegate.lastApplied
        set(value) {
            isMutated = true
            delegate.lastApplied = value
        }

    override var leaderId: PeerId
        get() = delegate.leaderId
        set(value) {
            isMutated = true
            delegate.leaderId = value
        }

    override val log = LogMutator(delegate.log)

    fun changeToFollower(term: Term) {
        isMutated = isMutated
                || currentTerm != term
                || votedFor != -1
        delegate.changeToFollower(term)
        changedToFollower = true
    }

    fun changeToLeader() {
        isMutated = isMutated
                || votedFor != -1
        delegate.changeToLeader()
        changedToLeader = true
    }

    fun changeToCandidate(me: PeerId) {
        isMutated = true
        delegate.changeToCandidate(me)
        changedToCandidate = true
    }

    fun updateClientIndexes(peerId: PeerId, newMatchIndex: Long) {
        delegate.updateClientIndexes(peerId, newMatchIndex)
    }

    fun decreaseNextIndex(peerId: PeerId) {
        delegate.decreaseNextIndex(peerId)
    }

    fun castVote(candidateId: PeerId) {
        isMutated = isMutated
                || votedFor != candidateId
        delegate.castVote(candidateId)
    }

    suspend fun updateCommitIndex() {
        if (role == Role.Leader) {
            ((commitIndex + 1)..java.lang.Long.MAX_VALUE)
                .lastWhileOrNull {
                    val a = matchIndexMajority(it)
                    val b = matchLastLogTerm(it)
                    a && b
                }
                ?.let {
                    commitIndex = it
                }
        }
    }

    suspend fun updateStateMachine(stateMachine: StateMachine<T>) {
        while (commitIndex > lastApplied) {
            val (committedValue, _) = log.get(lastApplied + 1)!!
            stateMachine.apply(committedValue)
            lastApplied++
        }
    }

    var isMutated: Boolean = false
        private set
        get() {
            return field || log.isMutated
        }

    var changedToFollower = false
    var changedToLeader = false
    var changedToCandidate = false

    suspend fun lastAppliedTerm(): LastAppliedTerm? =
        log.get(lastApplied)
            ?.term
            ?.let { LastAppliedTerm(lastApplied, it) }
}

class LogMutator<T>(private val delegate: Log<T>) : LogReader<T>(delegate) {

    suspend fun set(index: Long, item: T, term: Term) {
        isMutated = true
        delegate.set(index, item, term)
    }

    suspend fun append(item: T, term: Term): Long {
        isMutated = true
        return delegate.append(item, term)
    }

    suspend fun prune(index: Long) {
        log.info { "Pruning to $index" }
        isMutated = true
        delegate.prune(index)
    }

    var isMutated: Boolean = false
        private set
}

private inline fun <T> Iterable<T>.lastWhileOrNull(predicate: (T) -> Boolean): T? {
    var last: T? = null
    for (element in this) {
        if (!predicate(element)) {
            break
        }
        last = element
    }
    return last
}

suspend inline fun <T, V> changeState(
    name: String,
    state: State<T>,
    stateWriter: StateWriter<T>,
    stateMachine: StateMachine<T>,
    action: StateMutator<T>.() -> V,
): ChangeStateResult<V> =
    state.mutex.withLock {
        val mutator = StateMutator(state)
        val result = mutator.action()
        if (mutator.isMutated) {
            stateWriter.write(state)
        }
        mutator.updateCommitIndex()
        mutator.updateStateMachine(stateMachine)
        ChangeStateResult.fromStateMutator(result, mutator)
    }

data class LastAppliedTerm(val lastApplied: Long, val term: Term)
data class ChangeStateResult<V>(
    val value: V,
    val changedToFollower: Boolean,
    val changedToLeader: Boolean,
    val changedToCandidate: Boolean,
    val lastAppliedTerm: LastAppliedTerm?,
) {
    companion object {
        suspend fun <V, T> fromStateMutator(
            value: V,
            mutator: StateMutator<T>,
        ): ChangeStateResult<V> = ChangeStateResult(
            value,
            mutator.changedToFollower,
            mutator.changedToLeader,
            mutator.changedToCandidate,
            mutator.lastAppliedTerm(),
        )
    }
}
