package org.gertje.vlot

import org.gertje.vlot.state.State

interface StateWriter<T> {
    fun write(state: State<T>)
}
