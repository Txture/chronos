package org.chronos.chronodb.internal.impl.index

import org.chronos.chronodb.api.NullSortPosition
import org.chronos.chronodb.api.Order

class IndexValuesComparator(
    private val orders: List<Order>,
    private val nulls: List<NullSortPosition>
) : Comparator<Pair<String, List<Comparable<*>?>>> {

    init {
        require(this.orders.size == this.nulls.size) { "Orders and nulls must have the same length!" }
    }

    @Suppress("UNCHECKED_CAST")
    override fun compare(o1: Pair<String, List<Comparable<*>?>>, o2: Pair<String, List<Comparable<*>?>>): Int {
        val leftValues = o1.second
        val rightValues = o2.second
        if (leftValues.size != rightValues.size) {
            throw IllegalArgumentException(
                "Value list lengths do not match: left has ${leftValues.size} values, right has ${rightValues.size} values!"
            )
        }
        if (leftValues.size != this.orders.size) {
            throw IllegalArgumentException(
                "Value list length is ${leftValues.size}, but only ${this.orders.size} orderings were given!"
            )
        }
        for (index in leftValues.indices) {
            val leftValue = leftValues[index]
            val rightValue = rightValues[index]
            val order = orders[index]
            val nulls = nulls[index]
            if (leftValue == null && rightValue == null) {
                // tie; continue with next component
                continue
            } else if (leftValue != null && rightValue == null) {
                return when (nulls) {
                    NullSortPosition.NULLS_FIRST -> 1
                    NullSortPosition.NULLS_LAST -> -1
                }
            } else if (leftValue == null && rightValue != null) {
                return when (nulls) {
                    NullSortPosition.NULLS_FIRST -> -1
                    NullSortPosition.NULLS_LAST -> 1
                }
            } else {
                // the Kotlin compiler can't infer on its own that
                // both values are non-null here... let's give it some help.
                leftValue!!
                rightValue!!
                val comparisonResult = (leftValue as Comparable<Any>).compareTo(rightValue)
                if (comparisonResult == 0) {
                    // tie; continue with next pair of values in the list
                    continue
                }
                return when (order) {
                    // ascending is the default java comparison order, so "compareTo" will
                    // produce this order already.
                    Order.ASCENDING -> comparisonResult
                    // for descending, we need to invert the ordering given by "compareTo"
                    Order.DESCENDING -> comparisonResult * -1
                }
            }
        }
        // we are tied, break the tie with the primary key
        return o1.first.compareTo(o2.first)
    }

}