package org.gertje.vlot

interface ResettableTicker {
    fun start()
    fun reset()
    fun stop()
}
