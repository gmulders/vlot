package org.gertje.vlot

import kotlinx.coroutines.sync.withLock
import org.gertje.vlot.state.Log
import org.gertje.vlot.state.LogItem
import org.gertje.vlot.state.State
import org.gertje.vlot.state.getTerm

open class StateReader<T>(private val delegate: State<T>) {
    val role: Role
        get() = delegate.role
    val currentTerm: Term
        get() = delegate.currentTerm
    val votedFor: PeerId
        get() = delegate.votedFor
    open val commitIndex: Long
        get() = delegate.commitIndex
    open val lastApplied: Long
        get() = delegate.lastApplied
    val nextIndex: LongArray
        get() = delegate.nextIndex
    val matchIndex: LongArray
        get() = delegate.matchIndex
    val leaderId: PeerId
        get() = delegate.leaderId
    open val log: LogReader<T>
        get() = LogReader(delegate.log)

    protected fun matchIndexMajority(commitIndex: Long): Boolean =
        matchIndex.count { it >= commitIndex } + 1 > Math.floorDiv(delegate.clusterSize, 2)

    protected suspend fun matchLastLogTerm(index: Long): Boolean =
        log.get(index)?.term == currentTerm

    suspend fun isLogUpToDate(lastLogIndex: Long, lastLogTerm: Long): Boolean =
        delegate.log.get(lastLogIndex).getTerm() == lastLogTerm

    suspend fun logContainsEntry(index: Long, term: Term): Boolean =
        delegate.log.get(index).getTerm() == term
}

open class LogReader<T>(private val delegate: Log<T>) {
    suspend fun get(index: Long): LogItem<T>? = delegate.get(index)
    suspend fun slice(longRange: LongRange): List<LogItem<T>> = delegate.slice(longRange)

    val lastLogIndex: Long
        get() = delegate.lastLogIndex
    val lastLogTerm: Long
        get() = delegate.lastLogTerm
}

suspend inline fun <T, V> readState(state: State<T>, action: StateReader<T>.() -> V): V =
    state.mutex.withLock {
        val mutator = StateReader(state)
        mutator.action()
    }
