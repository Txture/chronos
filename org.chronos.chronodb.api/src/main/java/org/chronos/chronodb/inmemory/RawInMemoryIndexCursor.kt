package org.chronos.chronodb.inmemory

import org.chronos.chronodb.api.Order
import org.chronos.chronodb.internal.impl.index.cursor.RawIndexCursor

class RawInMemoryIndexCursor<V: Comparable<V>>(
    private val contents: Iterator<IndexEntry>,
    override val order: Order
): RawIndexCursor<V> {

    private var currentEntry: IndexEntry? = null

    override fun next(): Boolean {
        if(!this.contents.hasNext()){
            this.currentEntry = null
            return false
        }
        this.currentEntry = this.contents.next()
        return true
    }

    override val primaryKey: String
        get() = this.currentEntry?.key?.key
                ?: throw IllegalStateException("This cursor either hasn't been initialized or is exhausted!")

    @Suppress("UNCHECKED_CAST")
    override val indexValue: V
        get() =  this.currentEntry?.key?.indexValue as V?
            ?: throw IllegalStateException("This cursor either hasn't been initialized or is exhausted!")

    override fun isVisibleForTimestamp(timestamp: Long): Boolean {
        val periods  = this.currentEntry?.validPeriods
            ?: throw IllegalStateException("This cursor either hasn't been initialized or is exhausted!")
        for(period in periods){
            if(period.isAfter(timestamp)){
                // periods are sorted ascending by lower bound;
                // if the current period is after our timestamp,
                // all following periods will be after the timestamp
                // as well and none will match; there's no point in
                // iterating over the remaining periods.
                return false
            }
            if(period.contains(timestamp)){
                return true
            }
        }
        // none of the periods matched.
        return false
    }

    override fun close() {
        // inmemory, nothing to do here.
    }


}