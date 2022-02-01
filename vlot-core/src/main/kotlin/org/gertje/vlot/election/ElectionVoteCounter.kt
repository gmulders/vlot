package org.gertje.vlot.election

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.gertje.vlot.election.ElectionResult.*

class ElectionVoteCounter(voterCount: Int) {
    private var mutex: Mutex = Mutex()
    private var majority: Int = voterCount / 2 + 1
    private var yay: Int = 1
    private var nay: Int = 0

    suspend fun count(isYay: Boolean): ElectionResult = mutex.withLock {
        if (isYay) {
            yay++
        } else {
            nay++
        }
        return determineElectionResult()
    }

    private fun determineElectionResult(): ElectionResult {
        return if (nay < majority && yay < majority) {
            UNDETERMINED
        } else if (yay >= majority) {
            WON
        } else {
            LOST
        }
    }
}

enum class ElectionResult {
    UNDETERMINED,
    WON,
    LOST,
}
