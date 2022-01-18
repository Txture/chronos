package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order

/**
 * A [FilteringScanCursor] wraps an [IndexScanCursor] and filters it with the given [filter] predicate.
 */
class FilteringScanCursor<V: Comparable<V>>(
    private val cursor: IndexScanCursor<V>,
    private val filter: (Pair<V, String>) -> Boolean
) : IndexScanCursor<V>() {

    override val order: Order
        get() = this.cursor.order

    override val primaryKey: String
        get() = this.cursor.primaryKey

    override val indexValue: V
        get() = this.cursor.indexValue

    override fun nextInternal(): Boolean {
        if(!this.cursor.next()){
            return false
        }
        var currentEntry = Pair(cursor.indexValue, cursor.primaryKey)
        while(!filter(currentEntry)){
            if(!this.cursor.next()){
                return false
            }
            currentEntry = Pair(cursor.indexValue, cursor.primaryKey)
        }
        return true
    }

    override fun closeInternal() {
        this.cursor.close()
    }
}