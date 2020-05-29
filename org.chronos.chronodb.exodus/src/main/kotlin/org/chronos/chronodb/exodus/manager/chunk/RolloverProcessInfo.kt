package org.chronos.chronodb.exodus.manager.chunk

import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative

class RolloverProcessInfo(
        val oldHeadChunk: ChronoChunk,
        val newHeadChunk: ChronoChunk,
        val rolloverTimestamp: Long
) {

    init {
        requireNonNegative(this.rolloverTimestamp, "rolloverTimestamp")
    }

}