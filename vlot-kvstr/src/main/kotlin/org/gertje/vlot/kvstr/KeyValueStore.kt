package org.gertje.vlot.kvstr

import org.gertje.vlot.StateMachine

class KeyValueStore(
    private val delegate: MutableMap<String, Int> = mutableMapOf()
): Map<String, Int> by delegate, StateMachine<Pair<String, Int>> {

    override fun apply(pair: Pair<String, Int>) {
        val (key, value) = pair
        delegate[key] = value
    }
}
