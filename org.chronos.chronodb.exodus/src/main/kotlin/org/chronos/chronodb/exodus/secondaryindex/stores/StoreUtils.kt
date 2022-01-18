package org.chronos.chronodb.exodus.secondaryindex.stores

import jetbrains.exodus.ByteIterable

object StoreUtils {

    fun isTimestampInRange(timestamp: Long, ranges: ByteIterable, scanTimeMode: ScanTimeMode): Boolean {
        return when (scanTimeMode) {
            ScanTimeMode.SCAN_FOR_PERIOD_MATCHES -> rangeBinarySearch(ByteIterableLongSearchable(ranges), timestamp) != null
            ScanTimeMode.SCAN_FOR_TERMINATED_PERIODS -> rangeBinarySearchForHighestTerminatedPeriod(ByteIterableLongSearchable(ranges), timestamp) != null
        }
    }

}

