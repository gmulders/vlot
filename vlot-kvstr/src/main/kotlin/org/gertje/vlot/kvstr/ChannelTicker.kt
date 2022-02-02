package org.gertje.vlot.kvstr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.gertje.vlot.ResettableTicker
import kotlin.coroutines.CoroutineContext

class ChannelTicker(
    private val periodMillis: Long,
    private val tickOnStart: Boolean = false,
) : ResettableTicker, CoroutineScope {

    override val coroutineContext: CoroutineContext = Dispatchers.Default

    private val resetEvents = Channel<Unit>()
    val tickChannel = Channel<Unit>()

    var job: Job = Job().apply { cancel() }

    override fun start(): Unit = synchronized(this) {
        println("Start")
        if (job.isActive) {
            println("Still active")
            return
        }
        println("Launching new ticker coroutine")
        job = launch {
            if (tickOnStart) {
                tickChannel.send(Unit)
            }
            while (isActive) {
                select<Unit> {
                    resetEvents.onReceive { }
                    onTimeout(periodMillis) {
                        println("sending: $periodMillis")
                        tickChannel.send(Unit)
                        println("done sending: $periodMillis")
                    }
                }
                println("isActive: $isActive $periodMillis")
            }
        }
        println("Done launching new ticker coroutine")
    }

    override fun reset() = synchronized(this) {
        if (job.isActive) {
            launch {
                resetEvents.send(Unit)
            }
        }
    }

    override fun stop() = synchronized(this) {
        if (job.isActive) {
            job.cancel()
        }
    }
}
