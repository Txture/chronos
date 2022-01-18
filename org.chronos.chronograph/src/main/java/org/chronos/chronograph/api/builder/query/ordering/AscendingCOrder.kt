package org.chronos.chronograph.api.builder.query.ordering

import org.chronos.chronodb.api.NullSortPosition
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.TextCompare
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.largestComparable
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.smallestComparable

class AscendingCOrder(
    textCompare: TextCompare,
    nullSortPosition: NullSortPosition
) : AbstractCOrder(textCompare, nullSortPosition) {

    override fun getDirection(): Order {
        return Order.ASCENDING
    }

    override fun reversed(): COrder {
        return AscendingCOrder(this.textCompare, this.nullSortPosition.reversed())
    }

    override fun normalize(element: Any?): Any? {
        return when (element) {
            null -> null
            is Collection<*> -> element.smallestComparable()
            else -> element
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun compareNonNull(left: Any, right: Any): Int {
        if (left is String && right is String) {
            return when (this.textComp) {
                TextCompare.STRICT -> left.compareTo(right)
                TextCompare.CASE_INSENSITIVE -> left.compareTo(right, ignoreCase = true)
            }
        }
        if (left is Comparable<*> && right is Comparable<*>) {
            left as Comparable<Any>
            right as Comparable<Any>
            return left.compareTo(right)
        }
        throw IllegalStateException(
            "Cannot compare values for sorting." +
                " Left: $left (class: ${left::class.java.name})," +
                " Right: $right (class: ${right::class.java.name})"
        )
    }

}