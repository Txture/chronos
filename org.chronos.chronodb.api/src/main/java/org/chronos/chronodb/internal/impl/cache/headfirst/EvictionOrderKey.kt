package org.chronos.chronodb.internal.impl.cache.headfirst

import org.chronos.chronodb.internal.api.Period
import kotlin.math.abs

data class EvictionOrderKey(
    val branch: String,
    val keyspace: String,
    val key: String,
    val period: Period
) {

    companion object {

        fun createEvictionOrderComparator(preferredBranch: String?, preferredKeyspace: String?): Comparator<EvictionOrderKey> {
            return Comparator
                .comparing { key: EvictionOrderKey ->
                    if (key.branch == preferredBranch) {
                        // prefer this branch (order it to the beginning of the eviction list)
                        -1
                    } else {
                        // no preference; keep comparing other aspects
                        0
                    }
                }
                .thenComparing { key: EvictionOrderKey ->
                    if (key.period.isOpenEnded) {
                        // prefer this entry, as it is a HEAD revision entry (order it to the beginning of the eviction list)
                        -1
                    } else {
                        // no preference; keep comparing other aspects
                        0
                    }
                }
                .thenComparing { key: EvictionOrderKey ->
                    if (key.keyspace == preferredKeyspace) {
                        // prefer this entry, as it belongs to our preferred keyspace (order it to the beginning of the eviction list)
                        -1
                    } else {
                        // no preference; keep comparing to other aspects
                        0
                    }
                }
                // as a general rule of thumb, prefer newer entries over older ones.
                .thenComparing { key -> Long.MAX_VALUE - key.period.lowerBound }
                // the remaining comparisons here are merely for tie-breaking and serve no real ordering purpose anymore.
                .thenComparing { key -> key.period.upperBound }
                .thenComparing { key -> key.key }
        }

    }
}