package org.chronos.chronodb.exodus.manager.chunk.iterators

import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.exodus.kotlin.ext.parseAsUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.transaction.ExodusChunkTransaction
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry
import java.util.NoSuchElementException

class AllChunkEntriesIterator : AbstractCloseableIterator<UnqualifiedTemporalEntry> {

    private val tx: ExodusChunkTransaction
    private val minTimestamp: Long
    private val maxTimestamp: Long
    private val cursor: Cursor
    private val includeRolloverCommits: Boolean

    var next: UnqualifiedTemporalEntry? = null

    constructor(tx: ExodusChunkTransaction, storeName: String, minTimestamp: Long, maxTimestamp: Long, includeRollovers: Boolean) {
        require(minTimestamp >= 0) { "Precondition violation - argument 'minTimestamp' must be greater than or equal to zero!" }
        require(maxTimestamp >= 0) { "Precondition violation - argument 'maxTimestamp' must be greater than or equal to zero!" }
        require(minTimestamp <= maxTimestamp) { "Precondition violation - argument 'minTimestamp' must be less than or equal to 'maxTimestamp'!" }
        this.tx = tx
        this.minTimestamp = minTimestamp
        this.maxTimestamp = maxTimestamp
        this.cursor = tx.openCursorOn(storeName)
        this.includeRolloverCommits = includeRollovers
        this.tryMoveNext()
    }

    override fun next(): UnqualifiedTemporalEntry {
        if(!this.hasNext()){
            throw NoSuchElementException("Iterator is exhausted; there are no more elements!")
        }
        val next = this.next!!
        this.tryMoveNext()
        return next
    }

    override fun hasNextInternal(): Boolean {
        return this.next != null
    }

    override fun closeInternal() {
        this.cursor.close()
        this.tx.rollback()
    }

    private fun tryMoveNext(){
        while(this.cursor.next){
            val key = this.cursor.key
            // deserialize the key to check the timestamp on it
            val tKey = key.parseAsUnqualifiedTemporalKey()
            if(!this.includeRolloverCommits && tKey.timestamp == tx.chunkValidPeriod.lowerBound){
                // this is not an actual chunk entry but the result
                // of a rollover -> skip it
                continue
            }
            if (tKey.timestamp >= this.minTimestamp && tKey.timestamp <= this.maxTimestamp) {
                // found a matching key
                this.next = UnqualifiedTemporalEntry(tKey, cursor.value.toByteArray())
                return
            }
        }
        // no more elements
        this.next = null
    }

}