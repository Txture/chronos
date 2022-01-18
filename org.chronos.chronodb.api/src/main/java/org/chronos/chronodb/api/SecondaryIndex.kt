package org.chronos.chronodb.api

import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.index.IndexingOption
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import java.util.*

interface SecondaryIndex {

    val id: String
    val name: String
    val valueType: Class<*>
        get() {
            return when (this.indexer) {
                is StringIndexer -> String::class.java
                is LongIndexer -> Long::class.java
                is DoubleIndexer -> Double::class.java
                else -> throw IllegalStateException("Unknown Indexer type ${this.indexer.javaClass.name}")
            }
        }
    val indexer: Indexer<*>
    val validPeriod: Period
    val branch: String
    val parentIndexId: String?
    val dirty: Boolean
    val options: Set<IndexingOption>

    /**
     * Returns a copy of the options of this index which can be used for child indices.
     *
     * The returned set will only contain options which are inheritable.
     *
     * @return The options to use for child indices.
     */
    val inheritableOptions: Set<IndexingOption>
    get(){
        return this.options.asSequence().filter { !it.inheritable }.map { it.copy() }.toSet()
    }


    /**
     * Returns the indexed values for the given object.
     *
     * @param value    The object to index. May be `null`, which results in the empty set as the output.
     * @param index     The index to which the indexers belong. Used in error messages. Must not be `null`.
     * @return An immutable set, containing the index values calculated by running the given indexers on the given
     * object. May be empty, but never `null`.
     */
    @Suppress("UNCHECKED_CAST")
    fun getIndexedValuesForObject(value: Any?): Set<Comparable<*>> {
        if (value == null) {
            // when the object to index is null, we can't produce any indexed values.
            // empty set is already unmodifiable, no need to wrap it in Collections.umodifiableSet.
            return emptySet()
        }
        val indexer = this.indexer
        if (!indexer.canIndex(value)) {
            // no indexer applies to this object
            return emptySet()
        }
        val indexedValues = indexer.getIndexValues(value)
            ?: return emptySet()
        // make sure that there are no NULL values or empty values in the indexed values set
        return indexedValues.asSequence()
            .filter(IndexingUtils::isValidIndexValue)
            .toSet() as Set<Comparable<*>>
    }

}