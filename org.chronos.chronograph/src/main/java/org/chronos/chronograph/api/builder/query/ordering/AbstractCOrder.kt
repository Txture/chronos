package org.chronos.chronograph.api.builder.query.ordering

import org.chronos.chronodb.api.NullSortPosition
import org.chronos.chronodb.api.TextCompare

abstract class AbstractCOrder(
    protected val textComp: TextCompare,
    protected val nulls: NullSortPosition
) : COrder {

    override fun getTextCompare(): TextCompare {
        return this.textComp
    }

    override fun getNullSortPosition(): NullSortPosition {
        return this.nulls
    }

    protected abstract fun compareNonNull(left: Any, right: Any): Int

    override fun compare(first: Any?, second: Any?): Int {
        val left = normalize(first)
        val right = normalize(second)
        if (left == null && right == null) {
            return 0
        } else if (left == null && right != null) {
            return when (this.nulls) {
                NullSortPosition.NULLS_FIRST -> -1
                NullSortPosition.NULLS_LAST -> +1
            }
        } else if (left != null && right == null) {
            return when (this.nulls) {
                NullSortPosition.NULLS_FIRST -> +1
                NullSortPosition.NULLS_LAST -> -1
            }
        }
        // the kotlin compiler isn't smart enough to detect
        // that both values cannot be NULL here. Let's give it some help.
        left!!
        right!!
        return this.compareNonNull(left, right)
    }
}