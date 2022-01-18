package org.chronos.chronograph.internal.impl.optimizer.traversals

import com.google.common.collect.Table
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal
import org.apache.tinkerpop.gremlin.structure.Element
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.largestComparable
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.smallestComparable
import org.chronos.chronograph.api.builder.query.ordering.COrder
import java.util.Comparator

class TableLookupTraversal<S, C: Comparable<*>>(
    val lookupTable: Table<String, String, C>?,
    val propertyKey: String,
    val comparator: Comparator<C>
) : AbstractLambdaTraversal<S, C>() {

    private var e: C? = null

    override fun next(): C? {
        return e
    }

    @Suppress("UNCHECKED_CAST")
    override fun addStart(start: Traverser.Admin<S>) {
        val element = start.get()!!
        if (this.lookupTable == null) {
            // fall back to regular property access
            val indexedValueOrValues: Any? = when (element) {
                is Element -> element.property<Any>(this.propertyKey).orElse(null)
                is Map<*, *> -> (element as Map<String, *>)[this.propertyKey]
                else -> throw IllegalStateException(String.format(
                    "The by(\"%s\") modulator can only be applied to a traverser that is an Element or a Map - it is being applied to [%s] a %s class instead",
                    propertyKey, element, element.javaClass.simpleName))
            }
            this.e = this.normalizeValueForComparator(indexedValueOrValues)
        } else {
            element as Element
            val primaryKey = element.id()
            this.e = if (this.lookupTable.containsRow(primaryKey)) {
                this.lookupTable.get(primaryKey, this.propertyKey)
            } else {
                element.property<Any>(this.propertyKey).orElse(null) as C?
            }
        }
    }

    override fun toString(): String {
        return "TableLookup['${this.propertyKey}']"
    }

    override fun hashCode(): Int {
        return this.javaClass.hashCode() xor this.propertyKey.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return other is TableLookupTraversal<*, *> && other.propertyKey == this.propertyKey
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported here!")
    }

    @Suppress("UNCHECKED_CAST")
    private fun normalizeValueForComparator(value: Any?): C? {
        return when (value) {
            null -> null
            is Collection<*> -> when (this.comparator) {
                is COrder -> this.comparator.normalize(value)
                Order.asc -> value.smallestComparable()
                Order.desc -> value.largestComparable()
                Order.shuffle -> value.smallestComparable()
                // well if the user wants to use a custom comparator, they'll have to deal with collection values themselves.
                else -> value
            } as C?
            is Comparable<*> -> value as C?
            else -> throw IllegalStateException("Cannot convert value '${value}' of type '${value.javaClass.name}' to a comparable!")
        }
    }
}