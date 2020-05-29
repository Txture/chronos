package org.chronos.chronodb.exodus.manager.chunk.iterators

import org.chronos.chronodb.api.key.TemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.floorKey
import org.chronos.chronodb.exodus.kotlin.ext.parseAsInverseUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey

class ModificationsIterator : Iterator<TemporalKey> {

    private val chunkManager: GlobalChunkManager
    private val storeName: String
    private val keyspace: String
    private val period: Period
    private val chunkIterator: LazyChunkIterator<TemporalKey>

    constructor(chunkManager: GlobalChunkManager, storeName: String, keyspace: String, chunks: List<ChronoChunk>, period: Period) {
        this.chunkManager = chunkManager
        this.storeName = storeName
        this.keyspace = keyspace
        this.period = period
        this.chunkIterator = LazyChunkIterator(chunks.iterator(), this::createChunkElementIterator)
    }

    override fun hasNext(): Boolean {
        return this.chunkIterator.hasNext()
    }

    override fun next(): TemporalKey {
        return this.chunkIterator.next()
    }

    private fun createChunkElementIterator(chunk: ChronoChunk, isFirst: Boolean, isLast: Boolean): Iterator<TemporalKey> {
        // modifications iterators always operate in DESCENDING chunk order. Therefore,
        // the last chunk we receive is the origin chunk. In this origin chunk, we need
        // to include the rollover commit (if any) because that is our base line. For
        // all other chunks, we ignore the rollover commits (they are transparent to the user)
        var lowerBound: Long
        if (isLast) {
            lowerBound = Math.max(this.period.lowerBound, chunk.validPeriod.lowerBound)
        } else {
            // note: we EXCLUDE the exact lower bound of the chunk because this timestamp contains
            // the rollover data, which is not a "modification" that is visible to the user.
            lowerBound = Math.max(this.period.lowerBound + 1, chunk.validPeriod.lowerBound)
        }
        // note: upper bound is exclusive in our period, but inclusive in the "modifications between"
        // query, so we have to subtract 1 here.
        val upperBound = Math.min(this.period.upperBound - 1, chunk.validPeriod.upperBound)
        val list = chunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
            tx.openCursorOn(storeName).use { cursor ->
                val topKey = cursor.floorKey(InverseUnqualifiedTemporalKey.createMaxExclusive(upperBound).toByteIterable())
                if (topKey == null) {
                    // there are no changes in the store for the given period
                    listOf<TemporalKey>()
                } else {
                    val timestamps = mutableListOf<TemporalKey>()
                    var currentTemporalKey = topKey.parseAsInverseUnqualifiedTemporalKey().toTemporalKey(keyspace)
                    while(currentTemporalKey.timestamp >= lowerBound){
                        timestamps.add(currentTemporalKey)
                        if(!cursor.prev){
                            break
                        }
                        currentTemporalKey = cursor.key.parseAsInverseUnqualifiedTemporalKey().toTemporalKey(keyspace)
                    }
                    timestamps
                }
            }
        }
        return list.iterator()
    }
}