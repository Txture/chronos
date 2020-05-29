package org.chronos.chronodb.exodus.manager.chunk.iterators

import org.chronos.chronodb.api.Order
import org.chronos.chronodb.exodus.kotlin.ext.floorKey
import org.chronos.chronodb.exodus.kotlin.ext.parseAsUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey

class HistoryIterator : Iterator<Long> {

    private val chunkManager: GlobalChunkManager
    private val storeName: String
    private val lowerBound: Long
    private val upperBound: Long
    private val key: String
    private val order: Order
    private val chunkIterator: LazyChunkIterator<Long>

    constructor(chunkManager: GlobalChunkManager, storeName: String, chunks: List<ChronoChunk>, key: String, lowerBound: Long, upperBound: Long, order: Order) {
        this.chunkManager = chunkManager
        this.storeName = storeName
        this.lowerBound = lowerBound
        this.upperBound = upperBound
        this.key = key
        this.order = order
        this.chunkIterator = LazyChunkIterator(chunks.iterator(), this::createChunkElementIterator)
    }


    override fun hasNext(): Boolean {
        return this.chunkIterator.hasNext()
    }

    override fun next(): Long {
        return this.chunkIterator.next()
    }

    private fun createChunkElementIterator(chunk: ChronoChunk, isFirst: Boolean, isLast: Boolean): Iterator<Long> {
        // when iterating in ascending order, we need to flip "isFirst" and "isLast".
        val first: Boolean
        val last: Boolean
        when(order){
            Order.DESCENDING -> {
                first = isFirst
                last = isLast
            }
            Order.ASCENDING -> {
                first = isLast
                last = isFirst
            }
        }
        var list: List<Long> = chunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
            tx.openCursorOn(storeName).use { cursor ->
                val floorKey = cursor.floorKey(UnqualifiedTemporalKey.create(key, upperBound).toByteIterable())
                if (floorKey == null) {
                    // no entry in the store for the given key and timestamp -> history is empty
                    listOf()
                } else {
                    val history = mutableListOf<Long>()
                    val tkey = floorKey.parseAsUnqualifiedTemporalKey()
                    if (tkey.key != this.key || (tkey.timestamp == chunk.validPeriod.lowerBound)) {
                        //key not existent at the requested timestamp or we hit the border of the chunk
                        listOf()
                    } else {
                        //entry exists
                        history.add(tkey.timestamp)
                        while (cursor.prev) {
                            val prevKey = cursor.key.parseAsUnqualifiedTemporalKey()
                            if (prevKey.key != key || (prevKey.timestamp == chunk.validPeriod.lowerBound)) {
                                // we reached a different key or hit the border of the chunk -> history ends here
                                break
                            }
                            history.add(prevKey.timestamp)
                        }
                        history
                    }
                }
            }
        }
        // post process to see if we are in range
        list = list.filter { it >= this.lowerBound && it <= this.upperBound }
        // by default, we produce descending order here
        if(this.order == Order.ASCENDING){
            list = list.asReversed()
        }
        return list.iterator()
    }

}