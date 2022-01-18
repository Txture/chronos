package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order

/**
 * A [DeltaResolvingScanCursor] should be used when iterating over the secondary index of a branch.
 *
 * It takes an [IndexScanCursor] as [parent], and a [RawIndexCursor] together with a [timestamp],
 * and merges the entries from the parent (which should come from the parent branch) with the
 * entries from the child cursor. Duplicates (i.e. entries that appear both on the parent and the
 * child index) will be eliminated, and terminations in the child index will hide their counterparts
 * in the parent index.
 */
class DeltaResolvingScanCursor<V : Comparable<V>>(
    private val parent: IndexScanCursor<V>,
    private val timestamp: Long,
    private val cursor: RawIndexCursor<V>
) : IndexScanCursor<V>() {

    private var currentEntry: Pair<String, V>? = null
    private var parentExhausted: Boolean = false
    private var cursorExhausted: Boolean = false

    private var initialized: Boolean = false

    init {
        require(this.parent.order == this.cursor.order){
            "Cannot create DeltaResolvingScanCursor - the sort orders of the two input cursors are different." +
                " Parent = ${this.parent.order}, Cursor = ${this.cursor.order}!"
        }
    }

    override val order: Order
        get() = this.cursor.order  // parent and cursor order are always the same.

    override fun nextInternal(): Boolean {
        if (!initialized) {
            advanceParent()
            advanceCursor()
            initialized = true
        }
        this.currentEntry = this.computeNextEntry()
        return currentEntry != null
    }

    override fun closeInternal() {
        this.parent.close()
        this.cursor.close()
    }

    override val indexValue: V
        get() {
            return this.currentEntry?.second
                ?: throw IllegalStateException("The cursor is either exhausted or has not been initialized yet!")
        }

    override val primaryKey: String
        get() {
            return this.currentEntry?.first
                ?: throw IllegalStateException("The cursor is either exhausted or has not been initialized yet!")
        }

    private fun computeNextEntry(): Pair<String, V>? {
        if (parentExhausted && cursorExhausted) {
            return null
        }
        while (true) {
            val parentEntry = getEntryFromParent()
            val myEntry = getEntryFromCursor()
            return if (parentEntry == null) {
                if (myEntry == null) {
                    this.parentExhausted = true
                    this.cursorExhausted = true
                    null
                } else {
                    advanceCursor()
                    myEntry
                }
            } else {
                if (myEntry == null) {
                    advanceParent()
                    parentEntry
                } else {
                    // compare the pairs, first by index value, then by primary key.
                    var comparisonResult = this.compareEntries(parentEntry, myEntry)
                    // by default, the comparison result refers to ASCENDING sort. If
                    // we want DESCENDING, we'll have to invert the result.
                    if(this.order == Order.DESCENDING){
                        comparisonResult *= -1
                    }

                    if (comparisonResult < 0) {
                        // parentEntry < myEntry
                        advanceParent()
                        return parentEntry
                    } else if (comparisonResult > 0) {
                        // parentEntry > myEntry
                        val myEntryIsVisible = this.cursor.isVisibleForTimestamp(this.timestamp)
                        if (myEntryIsVisible) {
                            // return myEntry and advance cursor
                            advanceCursor()
                            return myEntry
                        } else {
                            // advance cursor & try again
                            advanceCursor()
                            continue
                        }
                    } else {
                        // parent entry == child entry
                        // if child entry has valid period lower bound which is less than
                        // this.timestamp, and the child entry is not visible, block the
                        // parent entry (because this entry has already been terminated
                        // in our index) and advance BOTH cursors, then try again.
                        val myEntryIsVisible = this.cursor.isVisibleForTimestamp(this.timestamp)
                        // advance both parent and cursor
                        advanceParent()
                        advanceCursor()
                        if (myEntryIsVisible) {
                            return myEntry
                        } else {
                            // the entry has been terminated in the child,
                            // check the next entry
                            continue
                        }
                    }
                }
            }
        }
    }

    private fun advanceCursor() {
        this.cursorExhausted = !this.cursor.next()
    }

    private fun advanceParent() {
        this.parentExhausted = !this.parent.next()
    }

    @Suppress("UNCHECKED_CAST")
    private fun getEntryFromCursor(): Pair<String, V>? {
        return if (cursorExhausted) {
            null
        } else {
            Pair(this.cursor.primaryKey, this.cursor.indexValue)
        }
    }

    private fun getEntryFromParent(): Pair<String, V>? {
        return if (parentExhausted) {
            null
        } else {
            Pair(parent.primaryKey, parent.indexValue)
        }
    }

    private fun compareEntries(left: Pair<String, V>, right: Pair<String, V>): Int {
        val valueComparison = left.second.compareTo(right.second)
        if (valueComparison != 0) {
            return valueComparison
        }
        return left.first.compareTo(right.first)
    }
}