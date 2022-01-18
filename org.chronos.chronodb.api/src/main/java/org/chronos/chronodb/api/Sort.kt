package org.chronos.chronodb.api

import org.chronos.chronodb.internal.impl.SortImpl

interface Sort {

    companion object {

        @JvmStatic
        fun by(indexName: String, order: Order): Sort {
            return SortImpl(indexName, order, TextCompare.DEFAULT, NullSortPosition.DEFAULT)
        }

        @JvmStatic
        fun by(indexName: String, order: Order, nulls: NullSortPosition): Sort {
            return SortImpl(indexName, order, TextCompare.DEFAULT, nulls)
        }

        @JvmStatic
        fun by(indexName: String, order: Order, textCompare: TextCompare): Sort {
            return SortImpl(indexName, order, textCompare, NullSortPosition.DEFAULT)
        }

        @JvmStatic
        fun by(indexName: String, order: Order, textCompare: TextCompare, nulls: NullSortPosition): Sort {
            return SortImpl(indexName, order, textCompare, nulls)
        }

    }

    fun thenBy(indexName: String, order: Order): Sort

    fun thenBy(indexName: String, order: Order, nulls: NullSortPosition): Sort

    fun thenBy(indexName: String, order: Order, textCompare: TextCompare): Sort

    fun thenBy(indexName: String, order: Order, textCompare: TextCompare, nulls: NullSortPosition): Sort

    fun getIndexNamesInOrder(): List<String>

    fun getSortOrderForIndex(indexName: String): Order

    fun getNullSortPositionForIndex(indexName: String): NullSortPosition

    fun getTextCompareForIndex(indexName: String): TextCompare

}