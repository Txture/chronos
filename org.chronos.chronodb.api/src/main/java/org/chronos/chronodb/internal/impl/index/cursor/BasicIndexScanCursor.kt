package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order

/**
 * Basic implementation of an [IndexScanCursor] that wraps a [RawIndexCursor] and filters the entries for the given [timestamp].
 */
class BasicIndexScanCursor<V : Comparable<V>>(
    private val cursor: RawIndexCursor<V>,
    private val timestamp: Long
) : IndexScanCursor<V>() {

    override val order: Order
        get() = this.cursor.order

    override val primaryKey: String
        get() = this.cursor.primaryKey

    override val indexValue: V
        get() = this.cursor.indexValue

    override fun nextInternal(): Boolean {
        while (true) {
            if (!this.cursor.next()) {
                return false
            }
            if (!this.cursor.isVisibleForTimestamp(this.timestamp)) {
                continue
            }
            return true
        }
    }

    override fun closeInternal() {
        this.cursor.close()
    }


}