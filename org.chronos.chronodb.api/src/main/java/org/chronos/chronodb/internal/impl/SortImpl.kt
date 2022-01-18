package org.chronos.chronodb.internal.impl

import org.chronos.chronodb.api.NullSortPosition
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.Sort
import org.chronos.chronodb.api.TextCompare

class SortImpl private constructor (
    private val parent: SortImpl?,
    private val indexName: String,
    private val order: Order,
    private val textCompare: TextCompare,
    private val nulls: NullSortPosition,
) : Sort {

    private val allIndexNamesInOrder: List<String>

    init {
        val parentIndexNames = parent?.getIndexNamesInOrder() ?: emptyList()
        if (parentIndexNames.contains(this.indexName)) {
            throw IllegalArgumentException("The index '$indexName' has already been used as sorting option!")
        }
        this.allIndexNamesInOrder = parentIndexNames + this.indexName
    }

    constructor(indexName: String, order: Order, textCompare: TextCompare, nulls: NullSortPosition)
        : this(null, indexName, order, textCompare, nulls)

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun thenBy(indexName: String, order: Order): Sort {
        return SortImpl(this, indexName, order, TextCompare.DEFAULT, NullSortPosition.DEFAULT)
    }

    override fun thenBy(indexName: String, order: Order, nulls: NullSortPosition): Sort {
        return SortImpl(this, indexName, order, TextCompare.DEFAULT, nulls)
    }

    override fun thenBy(indexName: String, order: Order, textCompare: TextCompare): Sort {
        return SortImpl(this, indexName, order, textCompare, NullSortPosition.DEFAULT)
    }

    override fun thenBy(indexName: String, order: Order, textCompare: TextCompare, nulls: NullSortPosition): Sort {
        return SortImpl(this, indexName, order, textCompare, nulls)
    }

    override fun getIndexNamesInOrder(): List<String> {
        return this.allIndexNamesInOrder
    }

    override fun getSortOrderForIndex(indexName: String): Order {
        if(this.indexName == indexName){
            return this.order
        }
        return this.parent?.getSortOrderForIndex(indexName)
            ?: throw IllegalArgumentException("The index name '${indexName}' is not part of this sort object!")
    }

    override fun getNullSortPositionForIndex(indexName: String): NullSortPosition {
        if(this.indexName == indexName){
            return this.nulls
        }
        return this.parent?.getNullSortPositionForIndex(indexName)
            ?: throw IllegalArgumentException("The index name '${indexName}' is not part of this sort object!")
    }

    override fun getTextCompareForIndex(indexName: String): TextCompare {
        if(this.indexName == indexName){
            return this.textCompare
        }
        return this.parent?.getTextCompareForIndex(indexName)
            ?: throw IllegalArgumentException("The index name '${indexName}' is not part of this sort object!")
    }

}