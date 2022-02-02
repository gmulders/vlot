package org.gertje.vlot.kvstr

import org.gertje.vlot.StateWriter
import org.gertje.vlot.Term
import org.gertje.vlot.state.Log
import org.gertje.vlot.state.LogItem
import org.gertje.vlot.state.State
import org.gertje.vlot.state.getTerm

class SimpleLog<T>: Log<T> {
    private val log = mutableListOf<LogItem<T>>()

    override suspend fun get(index: Long): LogItem<T>? {
        return if (index >= 0 && index < log.size) {
            log[index.toInt()]
        } else {
            null
        }
    }

    override suspend fun set(index: Long, item: T, term: Term) {
        val entry = LogItem(item, term)
        when {
            log.size == index.toInt() -> {
                log.add(entry)
            }
            log.size < index -> throw IllegalStateException("Should never happen")
            else -> {
                log[index.toInt()] = entry
            }
        }
    }

    override suspend fun append(item: T, term: Term): Long {
        log.add(LogItem(item, term))
        println("Appended: ${log.size}")
        return log.size.toLong() - 1
    }

    override suspend fun prune(index: Long) {
        if (index >= 0 && log.size > index) {
            log.subList(index.toInt(), log.size).clear()
        }
    }

    override suspend fun slice(longRange: LongRange): List<LogItem<T>> {
        return log.subList(longRange.first.toInt(), longRange.last.toInt() + 1)
    }

    override val lastLogIndex: Long
        get() = (log.size - 1).toLong()
    override val lastLogTerm: Long
        get() = log.lastOrNull().getTerm()
}

class NoStateWriter<T>: StateWriter<T> {
    override fun write(state: State<T>) {
        // We don't write anything
    }
}
