package org.chronos.chronodb.exodus.secondaryindex.stores

import jetbrains.exodus.ByteIterable
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.secondaryindex.*
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.util.readLong
import org.chronos.chronodb.exodus.util.readLongsFromBytes
import org.chronos.chronodb.exodus.util.writeLong
import org.chronos.chronodb.exodus.util.writeLongsToBytes
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification
import org.chronos.chronodb.internal.impl.index.cursor.BasicIndexScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor

abstract class SecondaryIndexStore<V, S : SearchSpecification<V, *>> {

    companion object {
        private const val LONG_BYTES = java.lang.Long.BYTES
    }

    fun insert(tx: ExodusTransaction, indexId: String, keyspace: String, indexValue: V, userKey: String, validFrom: Long, validTo: Long = Long.MAX_VALUE) {
        val presentValue = this.loadValue(tx, indexId, keyspace, indexValue, userKey)
        val longs = if (presentValue == null) {
            writeLongsToBytes(listOf(validFrom, validTo))
        } else {
            val presentBytes = presentValue.toByteArray()
            check(presentBytes.size % (LONG_BYTES * 2) == 0) { "Invariant error: list of timestamps is of uneven size! Index: '${indexId}', User Key: '${userKey}', Original Value: '${indexValue}'" }
            if (readLong(presentBytes, presentBytes.size - LONG_BYTES * 2) == Long.MAX_VALUE) {
                // the last period is open-ended, no need to change anything
                // (this should never happen and is just a safe-guard)
                return
            }
            // the new array is the old array plus two longs
            val newBytes = ByteArray(presentBytes.size + LONG_BYTES * 2)
            System.arraycopy(presentBytes, 0, newBytes, 0, presentBytes.size)
            writeLong(newBytes, validFrom, newBytes.size - 2 * LONG_BYTES)
            writeLong(newBytes, validTo, newBytes.size - 1 * LONG_BYTES)
            newBytes
        }
        val timestamps = longs.toByteIterable()
        this.storeEntry(tx, indexId, keyspace, indexValue, userKey, timestamps)
    }

    fun terminateValidity(tx: ExodusTransaction, indexId: String, keyspace: String, indexValue: V, userKey: String, timestamp: Long, lowerBound: Long): Boolean {
        val presentValue = loadValue(tx, indexId, keyspace, indexValue, userKey)
        if (presentValue == null) {
            // There is no entry for the old index value in our branch. This means that this indexed
            // value was never touched in our branch. To "simulate" a validity termination, we
            // insert a new index entry which is valid from the lower bound to our current timestamp.
            this.insert(tx, indexId, keyspace, indexValue, userKey, lowerBound, timestamp)
            return true
        } else {
            val presentBytes = presentValue.toByteArray()
            check(presentBytes.size % (LONG_BYTES * 2) == 0) { "Invariant error: list of timestamps is of uneven size! Index: '${indexId}', User Key: '${userKey}', Original Value: '${indexValue}'" }
            val lastPeriod = readLastPeriod(presentBytes)
            when {
                lastPeriod.lowerBound >= timestamp -> {
                    // the entry was created at the same timestamp where we are going
                    // to terminate it. That makes no sense, because the time ranges are
                    // inclusive in the lower bound and exclusive in the upper bound.
                    // Therefore, if lowerbound == upper bound, then the entry needs
                    // to be deleted instead. This situation can appear during incremental
                    // commits.
                    this.deleteValue(tx, indexId, keyspace, indexValue, userKey)
                    return true
                }
                lastPeriod.upperBound < Long.MAX_VALUE -> {
                    // the entry has already been closed. This can happen if a key-value pair has
                    // been inserted into the primary index, later deleted, and later re-inserted.
                    return false
                }
                lastPeriod.upperBound == Long.MAX_VALUE -> {
                    // terminate the previous period
                    writeLong(presentBytes, timestamp, presentBytes.size - LONG_BYTES)
                    val newBytes = presentBytes.toByteIterable()
                    this.storeEntry(tx, indexId, keyspace, indexValue, userKey, newBytes)
                    return true
                }
                else -> return false
            }
        }
    }

    /**
     * Rolls back the given index to the given timestamp.
     *
     * @param tx The transaction to operate on. Must not be `null`, must not be read-only.
     * @param indexName The name of the index to roll back. Must not be empty.
     * @param timestamp The timestamp to roll back to. Must not be negative.
     * @param keys The keys to roll back. Use `null` (default) to roll back all keys.
     */
    abstract fun rollback(tx: ExodusTransaction, indexName: String, timestamp: Long, keys: Set<QualifiedKey>? = null)

    private fun readLastPeriod(bytes: ByteArray): Period {
        val lowerBound = readLong(bytes, bytes.size - LONG_BYTES * 2)
        val upperBound = readLong(bytes, bytes.size - LONG_BYTES)
        require(lowerBound <= upperBound) {
            "Invariant violation - last period lower bound (${lowerBound}) is greater than its upper bound (${upperBound})!"
        }
        return Period.createRange(lowerBound, upperBound)
    }

    protected fun rollbackInternal(tx: ExodusTransaction, storeName: String, timestamp: Long, parseKey: (ByteArray) -> SecondaryIndexKey<V>, keys: Set<String>? = null) {
        tx.withCursorOn(storeName) { cursor ->
            while (cursor.next) {
                val indexKey = parseKey(cursor.key.toByteArray())
                if (keys != null && !keys.contains(indexKey.primaryKeyPlain)) {
                    // this key is not part of the set of keys to be rolled back; skip it
                    continue
                }
                val longs = readLongsFromBytes(cursor.value.toByteArray())
                val partitionedPairs = longs.asSequence().windowed(2, 2, false) { list -> Period.createRange(list[0], list[1]) }
                    .partition { period -> (period.contains(timestamp) && period.isOpenEnded) || (period.isBefore(timestamp) && !period.contains(timestamp)) }
                val okPeriods = partitionedPairs.first
                val problematicPeriods = partitionedPairs.second
                if (problematicPeriods.isNotEmpty()) {
                    // there are problematic periods, fix them
                    val firstProblematicPeriod = problematicPeriods.first()
                    if (firstProblematicPeriod.lowerBound > timestamp) {
                        // all of the problematic periods are after the timestamp -> drop them.
                    } else {
                        // this period contains the timestamp but was terminated, re-open it and mark it as ok
                        val fixedPeriod = firstProblematicPeriod.setUpperBound(Long.MAX_VALUE)
                        (okPeriods as MutableList).add(fixedPeriod)
                    }
                    // write the new periods
                    val newList = okPeriods.asSequence().flatMap { p -> listOf(p.lowerBound, p.upperBound).asSequence() }.toList()
                    val newValue = writeLongsToBytes(newList).toByteIterable()
                    tx.put(storeName, cursor.key, newValue)
                } else {
                    // there are no problematic periods, we're done
                    continue
                }
                if (okPeriods.isEmpty()) {
                    // the entire key-value pair needs to disappear from the store
                    tx.delete(storeName, cursor.key)
                }
            }
        }
    }

    protected fun allEntries(tx: ExodusTransaction, storeName: String, parseKey: (ByteArray) -> SecondaryIndexKey<V>, consumer: RawIndexEntryConsumer<V>) {
        tx.withCursorOn(storeName) { cursor ->
            while (cursor.next) {
                val indexKey = parseKey(cursor.key.toByteArray())
                val indexedValue = indexKey.indexValuePlain
                val primaryKey = indexKey.primaryKeyPlain
                val longs = readLongsFromBytes(cursor.value.toByteArray())
                // every pair of longs creates one period
                if (longs.size.rem(2) != 0) {
                    throw IllegalStateException("Found index entry with odd number of validity timestamps: ${primaryKey}->${indexedValue}, timestamps: ${longs}")
                }
                val periods = mutableListOf<Period>()
                for (i in 0 until longs.size step 2) {
                    val lowerBound = longs[i]
                    val upperBound = longs[i + 1]
                    periods += Period.createRange(lowerBound, upperBound)
                }
                consumer(storeName, primaryKey, indexedValue, periods)
            }
        }
    }


    abstract fun allEntries(tx: ExodusTransaction, keyspace: String, propertyName: String, consumer: RawIndexEntryConsumer<V>)

    abstract fun scan(tx: ExodusTransaction, searchSpec: S, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode = ScanTimeMode.SCAN_FOR_PERIOD_MATCHES): ScanResult<V>

    protected abstract fun storeEntry(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: V, userKey: String, timestamps: ByteIterable)

    protected abstract fun loadValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: V, userKey: String): ByteIterable?

    protected abstract fun deleteValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: V, userKey: String): Boolean

}