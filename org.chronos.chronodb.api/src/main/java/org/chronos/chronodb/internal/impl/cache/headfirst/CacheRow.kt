package org.chronos.chronodb.internal.impl.cache.headfirst

import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.internal.api.GetResult
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.cache.CacheGetResult
import org.chronos.chronodb.internal.impl.cache.mosaic.GetResultComparator
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class CacheRow(
    val rowKey: QualifiedKey
) {

    private val contents = TreeSet(GetResultComparator.getInstance())

    private val lock = ReentrantReadWriteLock(true)

    @Suppress("UNCHECKED_CAST")
    operator fun <T> get(timestamp: Long): CacheGetResult<T> {
        this.lock.readLock().withLock {
            for (result in this.contents) {
                val range = result.period
                if (range.contains(timestamp)) {
                    // cache hit, convert into a cache-get-result
                    val value = result.value as T
                    return CacheGetResult.hit(value, result.period.lowerBound)
                }
            }
            // cache miss
            return CacheGetResult.miss()
        }
    }

    fun delete(timestamp: Long): Boolean {
        this.lock.writeLock().withLock {
            var changed = false
            val iterator = this.contents.iterator()
            while (iterator.hasNext()) {
                val entry = iterator.next()
                if (entry.period.lowerBound == timestamp) {
                    iterator.remove()
                    changed = true
                }
            }
            return changed
        }
    }

    fun put(queryResult: GetResult<*>): Boolean {
        this.lock.writeLock().withLock {
            if (!queryResult.period.isOpenEnded) {
                // if there HAS been a previously (or concurrently) inserted
                // result which has an INFINITE upper bound and the same lower
                // bound as us, and our upper bound is limited, we must ensure
                // that the result with the infinite upper bound is removed
                // from the set, as our result is guaranteed to be more
                // up-to-date with respect to commits.
                val openEndedResult = GetResult.create(
                    queryResult.requestedKey,
                    queryResult.value,
                    Period.createOpenEndedRange(queryResult.period.lowerBound)
                )
                this.contents.remove(openEndedResult)
            }
            return this.contents.add(queryResult)
        }
    }

    /**
     * Writes the given value through this row, at the given timestamp.
     *
     * This method assumes that the given value will have a validity range that is open-ended on the righthand side.
     *
     * @param timestamp
     * The timestamp at which the write-through occurs. Must not be negative.
     * @param value
     * The value to write. May be `null` to indicate a deletion.
     */
    fun writeThrough(timestamp: Long, value: Any?) {
        this.lock.writeLock().withLock {
            // shorten the "valid to" period of the open-ended entry (if present) to the given timestamp
            limitOpenEndedPeriodEntryToUpperBound(timestamp)
            // create the new entry
            val newPeriod = Period.createOpenEndedRange(timestamp)
            val newEntry = GetResult.create(this.rowKey, value, newPeriod)
            this.contents.add(newEntry)
        }
    }

    /**
     * Rolls back this row to the specified timestamp, i.e. removes all entries with a validity range that is either
     * after, or contains, the given timestamp.
     *
     * @param timestamp The timestamp to roll back to. Must not be negative.
     * @return The total number of elements of the cache entries that were removed due to this operation. May be zero,
     * but never negative.
     */
    fun rollbackToTimestamp(timestamp: Long): Int {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' must not be negative, but is: ${timestamp}!" }
        this.lock.writeLock().withLock {
            val iterator: MutableIterator<GetResult<*>> = this.contents.iterator()
            var totalRemovedElements = 0
            while (iterator.hasNext()) {
                val entry = iterator.next()
                val range = entry.period
                if (range.isAfter(timestamp) || range.contains(timestamp)) {
                    totalRemovedElements += 1
                    iterator.remove()
                }
            }
            return totalRemovedElements
        }
    }

    /**
     * If this cache row contains an entry with a period that is open-ended on the righthand side, then this method
     * limits the upper bound of this period to the given value.
     *
     * If this row contains no entry with an open-ended period, this method has no effect.
     *
     * @param timestamp The timestamp to use as the new upper bound for the validity period, in case that an entry with an
     * open-ended period exists. Must not be negative.
     */
    private fun limitOpenEndedPeriodEntryToUpperBound(timestamp: Long) {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' must not be negative!" }
        this.lock.writeLock().withLock {
            // get the first entry (highest in cache, latest period) and check if its range needs to be trimmed
            val firstEntry = this.contents.pollFirst()
            if (firstEntry != null && firstEntry.period.upperBound > timestamp) {
                // the range of the entry needs to be trimmed to the current timestamp
                val range = firstEntry.period
                val newRange = range.setUpperBound(timestamp)
                val replacementEntry: GetResult<*> = GetResult.create(this.rowKey, firstEntry.value, newRange)
                this.contents.remove(firstEntry)
                this.contents.add(replacementEntry)
            }
        }
    }

    fun isEmpty(): Boolean {
        return this.contents.isEmpty()
    }

    fun size(): Int {
        return this.contents.size
    }

    override fun toString(): String {
        return "CacheRow[${this.rowKey}, size: ${this.size()}]"
    }
}