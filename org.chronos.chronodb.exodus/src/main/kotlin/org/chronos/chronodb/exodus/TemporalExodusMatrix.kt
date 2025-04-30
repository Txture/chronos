package org.chronos.chronodb.exodus

import com.google.common.collect.HashMultimap
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.algorithms.temporalGet
import org.chronos.chronodb.algorithms.temporalPut
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException
import org.chronos.chronodb.api.key.TemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.exodus.manager.chunk.iterators.AllEntriesIterator
import org.chronos.chronodb.exodus.manager.chunk.iterators.HistoryIterator
import org.chronos.chronodb.exodus.manager.chunk.iterators.ModificationsIterator
import org.chronos.chronodb.internal.api.GetResult
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.stream.CloseableIterator
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.chronos.chronodb.internal.util.KeySetModifications

class TemporalExodusMatrix : AbstractTemporalDataMatrix {

    // =================================================================================================================
    // STATIC
    // =================================================================================================================

    companion object {
        const val INVERSE_STORE_NAME_SUFFIX = "_inv"
    }


    private val chunkManager: GlobalChunkManager
    private val branchName: String
    val storeName: String

    val inverseStoreName
        get() = storeName + INVERSE_STORE_NAME_SUFFIX

    constructor(chunkManager: GlobalChunkManager, branchName: String, storeName: String, keyspace: String, creationTimestamp: Long) : super(keyspace, creationTimestamp) {
        this.chunkManager = chunkManager
        this.branchName = branchName
        this.storeName = storeName
    }

    override fun get(timestamp: Long, key: String): GetResult<ByteArray> {
        var floorEntry: Pair<ByteIterable, ByteIterable>? = null
        var higherEntry: Pair<ByteIterable, ByteIterable>? = null
        var chunkValidPeriod: Period = Period.eternal()
        val searchKey = UnqualifiedTemporalKey.create(key, timestamp).toByteIterable()
        chunkManager.openReadOnlyTransactionOn(branchName, timestamp).useChunkTx { tx ->
            tx.withCursorOn(storeName) { cursor ->
                val (floor, higher) = cursor.floorAndHigherEntry(searchKey)
                floorEntry = floor
                higherEntry = higher
            }
            chunkValidPeriod = tx.chunkValidPeriod
        }
        val floorEntry2 = floorEntry.mapSingle { Pair(it.first.parseAsUnqualifiedTemporalKey(), it.second.toByteArray()) }
        val higherEntry2 = higherEntry.mapSingle { Pair(it.first.parseAsUnqualifiedTemporalKey(), it.second.toByteArray()) }

        val getResult = temporalGet(keyspace, key, timestamp, floorEntry2, higherEntry2)
        // limit the range of the result to be within the bounds of this chunk
        return GetResult.alterPeriod(getResult, getResult.period.intersection(chunkValidPeriod))
    }

    override fun keySetModifications(timestamp: Long): KeySetModifications {
        return this.readFromStore(timestamp) { cursor ->
            val additions = mutableSetOf<String>()
            val removals = mutableSetOf<String>()
            while (cursor.next) {
                val key = cursor.key.parseAsUnqualifiedTemporalKey()
                if (key.timestamp > timestamp) {
                    // out of scope
                    continue
                }
                val userKey = key.key
                if (cursor.value.isEmpty()) {
                    // removal
                    additions.remove(userKey)
                    removals.add(userKey)
                } else {
                    // put
                    additions.add(userKey)
                    removals.remove(userKey)
                }
            }
            return@readFromStore KeySetModifications(additions, removals)
        }
    }

    override fun history(key: String, lowerBound: Long, upperBound: Long, order: Order): Iterator<Long> {
        var maxTime = upperBound
        if (maxTime < Long.MAX_VALUE) {
            // note: for this method, the "maxTime" is an inclusive value, which is why we add 1 here
            maxTime += 1
        }
        val branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName)
        var chunksForPeriod = branchChunkManager.getChunksForPeriod(Period.createRange(lowerBound, maxTime))
        // by default, chunks are sorted in ascending order. Check if we need descending
        if (order == Order.DESCENDING) {
            // descending order of chunk files
            chunksForPeriod = Lists.reverse(chunksForPeriod)
        }
        return HistoryIterator(this.chunkManager, this.storeName, chunksForPeriod, key, lowerBound, upperBound, order)
    }

    override fun allEntriesIterator(minTimestamp: Long, maxTimestamp: Long): CloseableIterator<UnqualifiedTemporalEntry> {
        // note: in the sense of the API of Chronos, we definitly want the rollover commits in the
        // result iterator, so we hard-code "true" for the second argument here. I leave the other
        // method (which takes the boolean as argument) for future use cases.
        return this.allEntriesIterator(minTimestamp, maxTimestamp, includeRolloverCommits = true)
    }

    fun allEntriesIterator(minTimestamp: Long, maxTimestamp: Long, includeRolloverCommits: Boolean): CloseableIterator<UnqualifiedTemporalEntry> {
        var upperBound = maxTimestamp
        if (upperBound < Long.MAX_VALUE) {
            // note: for this method, the "timestamp" is an inclusive value, which is why we add 1 here
            upperBound += 1
        }
        val branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val chunksForPeriod = branchChunkManager.getChunksForPeriod(Period.createRange(minTimestamp, upperBound))
        return AllEntriesIterator(this.chunkManager, chunksForPeriod, this.storeName, minTimestamp, maxTimestamp, includeRolloverCommits)
    }

    override fun lastCommitTimestamp(key: String, upperBound: Long): Long {
        val history = this.history(key, 0L, upperBound, Order.DESCENDING)
        return history.nextOrElse(-1)
    }

    override fun getModificationsBetween(timestampLowerBound: Long, timestampUpperBound: Long): Iterator<TemporalKey> {
        var upperBound = timestampUpperBound
        if (upperBound < Long.MAX_VALUE) {
            // note: for this operation, the upper bound is inclusive, which is why we add 1
            upperBound += 1
        }
        val period = Period.createRange(timestampLowerBound, upperBound)
        val branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName)
        var chunksForPeriod = branchChunkManager.getChunksForPeriod(period)
        // descending order of chunk files
        chunksForPeriod = Lists.reverse(chunksForPeriod)
        return ModificationsIterator(this.chunkManager, this.inverseStoreName, this.keyspace, chunksForPeriod, period)
    }


    override fun size(): Long {
        return this.chunkManager.openReadOnlyTransactionOnHeadChunkOf(this.branchName).use { tx ->
            tx.storeSize(this.storeName)
        }
    }

    override fun put(timestamp: Long, contents: Map<String, ByteArray>) {
        require(timestamp >= 0){ "Precondition violation - argument 'timestamp' must not be negative!" }
        if(contents.isEmpty()){
            return
        }
        this.ensureCreationTimestampIsGreaterThanOrEqualTo(timestamp)
        temporalPut(timestamp, contents) { entries ->
            chunkManager.openReadWriteTransactionOnHeadChunkOf(this.branchName).use { tx ->
                val store = this.storeName
                val inverseStore = this.inverseStoreName
                entries.forEach {
                    tx.put(store, it.key.toByteIterable(), it.value.toByteIterable())
                    tx.put(inverseStore, it.inverseKey.toByteIterable(), it.inverseValue.toByteIterable())
                }
                tx.commit()
            }
        }
    }

    override fun ensureCreationTimestampIsGreaterThanOrEqualTo(timestamp: Long) {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' must not be negative!" }
        if (this.creationTimestamp > timestamp) {
            this.creationTimestamp = timestamp
            this.chunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
                NavigationIndex.insert(tx, this.branchName, this.keyspace, this.storeName, timestamp)
                tx.commit()
            }
        }
    }

    override fun insertEntries(entries: Set<UnqualifiedTemporalEntry>, force: Boolean) {
        if(entries.isEmpty()){
            return
        }
        val minTimestamp = entries.asSequence().map { it.key.timestamp }.minOrNull()
        if (minTimestamp != null) {
            this.ensureCreationTimestampIsGreaterThanOrEqualTo(minTimestamp)
        }
        if (force) {
            // forced insertion is allowed in ALL chunks
            this.insertEntriesIntoChunks(entries)
        } else {
            // if the insertion is not forced, insertion is only allowed in the HEAD chunk
            this.insertEntriesIntoHead(entries)
        }
    }

    private fun insertEntriesIntoChunks(entries: Set<UnqualifiedTemporalEntry>) {
        val bcm = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val chunkToEntries = HashMultimap.create<ChronoChunk, UnqualifiedTemporalEntry>()
        // group the entries by chunk
        for (entry in entries) {
            val chunk = bcm.getChunkForTimestamp(entry.key.timestamp)!!
            chunkToEntries.put(chunk, entry)
        }
        for (chunk in chunkToEntries.keySet()) {
            val chunkEntries = chunkToEntries.get(chunk)
            // perform transactional insert
            this.chunkManager.openReadWriteTransactionOn(chunk).use { tx ->
                ExodusDataMatrixUtil.insertEntries(tx, this.storeName, chunkEntries)
                tx.commit()
            }
        }
    }

    private fun insertEntriesIntoHead(entries: Set<UnqualifiedTemporalEntry>) {
        val branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val chunk = branchChunkManager.headChunk
        val chunkPeriod = chunk.validPeriod
        // check if all entries are within head revision bounds
        for (entry in entries) {
            if (!chunkPeriod.contains(entry.key.timestamp)) {
                throw IllegalStateException("Entry at '${entry.key}' is out of bounds of head revision chunk")
            }
        }
        this.chunkManager.openReadWriteTransactionOnHeadChunkOf(this.branchName).use { tx ->
            ExodusDataMatrixUtil.insertEntries(tx, this.storeName, entries)
            tx.commit()
        }
    }


    override fun rollback(timestamp: Long) {
        val branchChunkManager = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val chunk = branchChunkManager.getChunkForTimestamp(timestamp)
        if (chunk == null) {
            throw IllegalStateException("There is no chunk for rollback to timestamp ${timestamp} in branch ${this.branchName}!")
        }
        val chunkFilePeriod = chunk.validPeriod
        if (!chunkFilePeriod.isOpenEnded) {
            throw IllegalStateException("Cannot roll back! Timestamp '$timestamp' is not in head revision.")
        }
        if (!chunkFilePeriod.contains(timestamp) || chunkFilePeriod.lowerBound > timestamp) {
            throw IllegalStateException("Timestamp '$timestamp' not within chunk!")
        }
        this.chunkManager.openReadWriteTransactionOnHeadChunkOf(this.branchName).use { tx ->
            tx.openCursorOn(this.storeName).use { cursor ->
                while (cursor.next) {
                    val key = cursor.key.parseAsUnqualifiedTemporalKey()
                    if (key.timestamp > timestamp) {
                        val success = cursor.deleteCurrent()
                        if (!success) {
                            throw IllegalStateException("Failed to delete entry ${key}!")
                        }
                    }
                }
            }
            tx.openCursorOn(this.inverseStoreName).use { cursor ->
                val higherKey = cursor.higherKey(InverseUnqualifiedTemporalKey.createMinInclusive(timestamp).toByteIterable())
                if (higherKey != null) {
                    do {
                        cursor.deleteCurrent()
                    } while (cursor.next)
                }
            }
            tx.commit()
        }
    }

    override fun purgeEntries(keys: MutableSet<UnqualifiedTemporalKey>): Int {
        if (keys.isEmpty()) {
            return 0
        }
        val bcm = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val affectedChunks = keys.asSequence().map(UnqualifiedTemporalKey::getTimestamp).distinct().mapNotNull(bcm::getChunkForTimestamp).toList()
        val chunkToKeys = keys.groupBy { key -> affectedChunks.find { it.validPeriod.contains(key.timestamp) } ?: throw IllegalStateException("No chunk for timestamp ${key.timestamp} on branch ${this.branchName}!") }
        var successfullyPurged = 0
        for ((chunk, localKeys) in chunkToKeys) {
            this.chunkManager.openReadWriteTransactionOn(chunk).use { tx ->
                for (tKey in localKeys) {
                    val binaryKey = tKey.toByteIterable()
                    val inverseKey = tKey.inverse().toByteIterable()
                    var deleted = tx.delete(this.storeName, binaryKey)
                    deleted = tx.delete(this.inverseStoreName, inverseKey) || deleted
                    if (deleted) {
                        successfullyPurged++
                    }
                }
                tx.commit()
            }
        }
        return successfullyPurged
    }

    override fun purgeAllEntriesInTimeRange(purgeRangeStart: Long, purgeRangeEnd: Long): MutableSet<UnqualifiedTemporalKey> {
        require(purgeRangeStart <= purgeRangeEnd) { "Precondition violation - argument 'purgeRangeStart' must be less than or equal to 'purgeRangeEnd'!" }
        val period = if (purgeRangeEnd < Long.MAX_VALUE) {
            // note that the argument 'purgeRangeEnd' is inclusive, but the period upper bound is
            // exclusive, so we add 1.
            Period.createRange(purgeRangeStart, purgeRangeEnd + 1)
        } else {
            Period.createOpenEndedRange(purgeRangeStart)
        }
        val bcm = this.chunkManager.getChunkManagerForBranch(this.branchName)
        val purgedKeys = Sets.newHashSet<UnqualifiedTemporalKey>()
        for (chunk in bcm.getChunksForPeriod(period)) {
            this.chunkManager.openReadWriteTransactionOn(chunk).use { tx ->
                val keysToRemoveInChunk = Lists.newArrayList<UnqualifiedTemporalKey>()
                val lowerBound = InverseUnqualifiedTemporalKey.create(purgeRangeStart, "").toByteIterable()
                val upperBound = InverseUnqualifiedTemporalKey.create(purgeRangeEnd + 1, "").toByteIterable()
                tx.withCursorOn(this.inverseStoreName) { cursor ->
                    if (cursor.ceilKey(lowerBound) == null) {
                        return@withCursorOn
                    }
                    while (cursor.key < upperBound) {
                        val key = cursor.key.parseAsInverseUnqualifiedTemporalKey()

                        // do not delete chunk boundaries
                        if (key.timestamp != chunk.validPeriod.lowerBound) {
                            // println("DELETING " + cursor.key.parseAsInverseUnqualifiedTemporalKey() + " FROM CHUNK " + chunk)
                            val deleted = cursor.deleteCurrent()
                            if (!deleted) {
                                throw ChronoDBStorageBackendException("Failed to perform Purge Entries in Range - deletion of key failed: ${key}")
                            }
                            keysToRemoveInChunk.add(key.inverse())
                        }

                        val hasNext = cursor.next
                        if (!hasNext) {
                            break
                        }
                    }
                }
                for (key in keysToRemoveInChunk) {
                    tx.delete(this.storeName, key.toByteIterable())
                }
                purgedKeys.addAll(keysToRemoveInChunk)
                tx.commit()
            }
        }
        return purgedKeys
    }


    private fun <T> readFromStore(timestamp: Long, consumer: (Cursor) -> T): T {
        return this.chunkManager.openReadOnlyTransactionOn(this.branchName, timestamp).use { tx ->
            tx.withCursorOn(this.storeName, consumer)
        }
    }

    private fun <T> readFromInverseStore(timestamp: Long, consumer: (Cursor) -> T): T {
        return this.chunkManager.openReadOnlyTransactionOn(this.branchName, timestamp).use { tx ->
            tx.withCursorOn(this.inverseStoreName, consumer)
        }
    }

}


