package org.chronos.chronodb.exodus.secondaryindex.stores

fun rangeBinarySearch(ranges: LongSearchable, timestamp: Long): Pair<Long, Long>? {
    require(ranges.size % 2 == 0) { "Precondition violation - argument 'ranges' must be of even length!" }
    if (ranges.isEmpty()) {
        return null
    }
    var lowestRangeIndex = 0
    var highestRangeIndex = ranges.size / 2 - 1
    while (true) {
        if (lowestRangeIndex > highestRangeIndex) {
            return null
        } else if (lowestRangeIndex == highestRangeIndex) {
            val lowerBound = getLowerBoundForRangeIndex(ranges, lowestRangeIndex)
            val upperBound = getUpperBoundForRangeIndex(ranges, highestRangeIndex)
            if (timestamp in lowerBound until upperBound) {
                return Pair(lowerBound, upperBound)
            } else {
                return null
            }
        }
        val halfwayRangeIndex = lowestRangeIndex + (highestRangeIndex - lowestRangeIndex) / 2
        val halfwayPointLower = getLowerBoundForRangeIndex(ranges, halfwayRangeIndex)
        val halfwayPointUpper = getUpperBoundForRangeIndex(ranges, halfwayRangeIndex)
        if (timestamp in halfwayPointLower until halfwayPointUpper) {
            return Pair(halfwayPointLower, halfwayPointUpper)
        }
        if (halfwayPointLower > timestamp) {
            highestRangeIndex = halfwayRangeIndex - 1
        } else if (halfwayPointUpper <= timestamp) {
            lowestRangeIndex = halfwayRangeIndex + 1
        } else {
            throw IllegalStateException("Unreachable.")
        }
    }
}

/**
 * Searches the given (ascending sorted) array of ranges and returns the largest terminated period before the given timestamp, or `null` if the timestamp is either in a period (regardless if terminated or not) or there is no period before the given timestamp.
 *
 * @param ranges The ranges to search through. Must be of even length (lower bound, upper bound alternating). Assumed to be sorted in ascending (lower bound) order. Must not overlap.
 * @param timestamp The timestamp to search for.
 * @return The highest terminated range as a pair of (lower bound, upper bound), or `null` if there is no such range.
 */
fun rangeBinarySearchForHighestTerminatedPeriod(ranges: LongSearchable, timestamp: Long): Pair<Long, Long>? {
    require(ranges.size % 2 == 0) { "Precondition violation - argument 'ranges' must be of even length!" }
    if (ranges.isEmpty()) {
        return null
    }
    var lowestRangeIndex = 0
    var highestRangeIndex = ranges.size / 2 - 1
    while (true) {
        if (lowestRangeIndex == highestRangeIndex) {
            val lowerBound = getLowerBoundForRangeIndex(ranges, lowestRangeIndex)
            val upperBound = getUpperBoundForRangeIndex(ranges, highestRangeIndex)
            if (lowerBound <= timestamp && timestamp < upperBound) {
                return null // timestamp is within a period
            } else {
                if(timestamp >= upperBound){
                    // arrived at the goal
                    return Pair(lowerBound, upperBound)
                }else{
                    // the current period is too late -> use the previous one (if any)
                    if(lowestRangeIndex > 0){
                        val from = getLowerBoundForRangeIndex(ranges, lowestRangeIndex -1)
                        val to = getUpperBoundForRangeIndex(ranges, lowestRangeIndex -1)
                        return Pair(from, to)
                    }else{
                        return null
                    }
                }
            }
        }
        val halfwayRangeIndex = lowestRangeIndex + (highestRangeIndex - lowestRangeIndex) / 2
        val halfwayPointLower = getLowerBoundForRangeIndex(ranges, halfwayRangeIndex)
        val halfwayPointUpper = getUpperBoundForRangeIndex(ranges, halfwayRangeIndex)
        if (halfwayPointLower <= timestamp && timestamp < halfwayPointUpper) {
            // we have found a period that contains the timestamp...
            return null
        }
        if (halfwayPointLower > timestamp) {
            highestRangeIndex = halfwayRangeIndex - 1
        } else if (halfwayPointUpper <= timestamp) {
            lowestRangeIndex = halfwayRangeIndex + 1
        } else {
            throw IllegalStateException("Unreachable.")
        }
    }
}

private fun getLowerBoundForRangeIndex(ranges: LongSearchable, rangeIndex: Int): Long {
    return ranges[rangeIndex * 2]
}

private fun getUpperBoundForRangeIndex(ranges: LongSearchable, rangeIndex: Int): Long {
    return ranges[rangeIndex * 2 + 1]
}

data class BinarySearchResult(
        val index: Long,
        val lowerBound: Long,
        val upperBound: Long
)
