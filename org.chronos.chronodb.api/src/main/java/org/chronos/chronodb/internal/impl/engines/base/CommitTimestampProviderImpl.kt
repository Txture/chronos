package org.chronos.chronodb.internal.impl.engines.base

import org.chronos.chronodb.internal.api.CommitTimestampProvider
import kotlin.math.max

class CommitTimestampProviderImpl : CommitTimestampProvider {

    private var lastServedTimestamp: Long

    constructor(maxNowAcrossAllBranches: Long) {
        this.lastServedTimestamp = maxNowAcrossAllBranches
    }

    @Synchronized
    override fun getNextCommitTimestamp(nowOnBranch: Long): Long {
        // wait for the next available timestamp
        while (System.currentTimeMillis() <= max(nowOnBranch, this.lastServedTimestamp)) {
            try {
                Thread.sleep(max(1, max(nowOnBranch, this.lastServedTimestamp) - System.currentTimeMillis()))
            } catch (ignored: InterruptedException) {
                // raise the interrupt flag again
                Thread.currentThread().interrupt()
            }
        }
        this.lastServedTimestamp = System.currentTimeMillis()
        return lastServedTimestamp
    }


}