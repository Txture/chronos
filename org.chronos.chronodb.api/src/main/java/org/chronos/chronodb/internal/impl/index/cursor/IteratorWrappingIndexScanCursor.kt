package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order

/**
 * An [IteratorWrappingIndexScanCursor] implements the [IndexScanCursor] interface based on the entries provided by an [Iterator].
 */
class IteratorWrappingIndexScanCursor<V : Comparable<V>>(
    private val iterator: Iterator<Pair<V, String>>,
    override val order: Order
) : IndexScanCursor<V>() {

    private var currentEntry: Pair<V, String>? = null

    override fun nextInternal(): Boolean {
        assertNotClosed()
        if (!this.iterator.hasNext()) {
            this.currentEntry = null
            return false
        }
        this.currentEntry = this.iterator.next()
        return true
    }

    override val primaryKey: String
        get() {
            this.assertNotClosed()
            return this.currentEntry?.second
                ?: throw IllegalStateException("The cursor is either exhausted or has not been initialized yet!")
        }

    override val indexValue: V
        get() {
            this.assertNotClosed()
            return this.currentEntry?.first
                ?: throw IllegalStateException("The cursor is either exhausted or has not been initialized yet!")
        }


    override fun closeInternal() {
    }


}