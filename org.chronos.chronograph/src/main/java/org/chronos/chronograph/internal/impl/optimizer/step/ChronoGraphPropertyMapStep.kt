package org.chronos.chronograph.internal.impl.optimizer.step

import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import org.chronos.chronograph.api.structure.ChronoEdge
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.api.structure.ChronoVertex
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.*
import java.util.concurrent.Callable

/**
 * A specialized version of [PropertyMapStep].
 *
 * The main difference is that this class may receive elements for prefetching via [registerFutureElementForPrefetching]. This can be
 * called from other steps and/or graph traversal strategies and will increase performance if the [propertyKeys] are indexed.
 */
class ChronoGraphPropertyMapStep<K, E> : ScalarMapStep<Element, MutableMap<K, E>>, TraversalParent, ByModulating, Configuring, Prefetching {

    val propertyKeys: Array<out String>
    val propertyType: PropertyType

    private var tokens = 0
    private var propTraversal: Traversal.Admin<Element, out Property<*>>? = null

    private val parameters = Parameters()
    private var traversalRing: TraversalRing<K, E>

    private val prefetcher: Prefetcher


    constructor(
        traversal: Traversal.Admin<*, *>?,
        propertyType: PropertyType,
        includedTokens: Int,
        propertyKeys: Array<out String>,
        labels: Set<String>,
    ) : super(traversal) {
        this.propertyKeys = propertyKeys
        this.propertyType = propertyType
        this.tokens = includedTokens
        this.labels.addAll(labels)
        this.propTraversal = null
        this.traversalRing = TraversalRing()
        this.prefetcher = Prefetcher(this.traversal, propertyKeys)
        this.prefetcher.addCustomPrefetchingCondition {
            // some options are not supported in indexed mode.
            !this.includesAnyTokenOf(WithOptions.labels, WithOptions.keys, WithOptions.list)
        }
    }

    override fun registerFutureElementForPrefetching(element: Element) {
        this.prefetcher.registerFutureElementForPrefetching(element)
    }

    @Suppress("UNCHECKED_CAST")
    override fun map(traverser: Traverser.Admin<Element>): MutableMap<K, E> {
        val element = traverser.get()
        if (element is ChronoElement && !element.isLazy) {
            // the element is new in this transaction, or modified, or...
            // in any case, it is not "clean" and therefore the secondary index is useless.
            return this.mapWithStandardAlgorithm(traverser)
        }

        val cacheResultRow = this.prefetcher.getPrefetchResult(element)
            ?: return this.mapWithStandardAlgorithm(traverser) // we have no prefetch cache

        val primaryKey = traverser.get().id() as String
        val map = if (cacheResultRow.isEmpty()) {
            // cache miss. This can happen if we received no call
            // to "registerFutureElementForPrefetching" for this element. It's not
            // critical per se, because we have a fallback, but doing this too often
            // may result in suboptimal performance.
            return this.mapWithStandardAlgorithm(traverser)
        } else {
            // cache hit, all good.
            this.convertTableRowToResultMap(cacheResultRow)
        }
        if (this.includesToken(WithOptions.ids)) {
            map[T.id] = primaryKey
        }
        for (propertyKey in this.propertyKeys) {
            map.putIfAbsent(propertyKey, emptyList<Any>())
        }
        return map as MutableMap<K, E>
    }

    @Suppress("UNCHECKED_CAST")
    private fun mapWithStandardAlgorithm(traverser: Traverser.Admin<Element>): MutableMap<K, E> {
        val map: MutableMap<Any, Any> = LinkedHashMap()
        val element = traverser.get()
        if (propertyType == PropertyType.VALUE) {
            if (includesToken(WithOptions.ids)) {
                map[T.id] = element.id()
            }
            if (element is VertexProperty<*>) {
                if (includesToken(WithOptions.keys)) {
                    map[T.key] = element.key()
                }
                if (includesToken(WithOptions.values)) {
                    map[T.value] = element.value()
                }
            } else {
                if (includesToken(WithOptions.labels)) {
                    map[T.label] = element.label()
                }
            }
        }
        val properties = if (null == propTraversal) {
            element.properties<Any>(*propertyKeys)
        } else {
            TraversalUtil.applyAll(traverser, propTraversal)
        }
        while (properties.hasNext()) {
            val property = properties.next()
            val value = if (propertyType === PropertyType.VALUE) property.value() else property
            if (element is Vertex) {
                map.compute(property.key()) { _: Any?, v: Any? ->
                    val values = if (v != null) {
                        v as MutableSet<Any>
                    } else {
                        mutableSetOf()
                    }
                    when (value) {
                        null -> {
                            /* ignore */
                        }

                        is Collection<*> -> values.addAll(value as Collection<Any>)
                        else -> values.add(value)
                    }
                    values
                }
            } else {
                map[property.key()] = value
            }
        }
        if (!traversalRing.isEmpty) {
            for (key in map.keys) {
                map.compute(key) { _: Any?, v: Any? ->
                    TraversalUtil.applyNullable(v, traversalRing.next() as Traversal.Admin<Any, Any>)
                }
            }
            traversalRing.reset()
        }
        // gremlin demands lists as values, not sets.
        map.replaceAll { _, value ->
            if (value is Collection<*>) {
                value.toList()
            } else {
                value
            }
        }

        // if there were any cells without values, fill them with empty lists.
        for (propertyKey in this.propertyKeys) {
            map.putIfAbsent(propertyKey, emptyList<Any>())
        }
        return map as MutableMap<K, E>
    }


    private fun convertTableRowToResultMap(row: Map<String, Set<Comparable<*>>>): MutableMap<Any, Any> {
        val result = mutableMapOf<Any /*T.id or property name */, Any /* index value */>()
        for ((key, value) in row.entries) {
            // gremlin requires the values to be lists, not sets.
            result[key] = value.toList()
        }
        return result
    }


    override fun configure(vararg keyValues: Any) {
        if (keyValues[0] == WithOptions.tokens) {
            if (keyValues.size == 2 && keyValues[1] is Boolean) {
                tokens = if (keyValues[1] as Boolean) WithOptions.all else WithOptions.none
            } else {
                for (i in 1 until keyValues.size) {
                    require(keyValues[i] is Int) {
                        "WithOptions.tokens requires Integer arguments (possible " + "" +
                            "values are: WithOptions.[none|ids|labels|keys|values|all])"
                    }
                    tokens = tokens or keyValues[i] as Int
                }
            }
        } else {
            parameters.set(this, *keyValues)
        }
    }

    override fun getParameters(): Parameters {
        return parameters
    }

    @Suppress("UNCHECKED_CAST")
    override fun <S : Any?, E : Any?> getLocalChildren(): List<Traversal.Admin<S, E>> {
        val result = mutableListOf<Traversal.Admin<*, *>>()
        if (null != propTraversal) {
            result.add(propTraversal as Traversal.Admin<*, *>)
        }
        result.addAll(traversalRing.traversals)
        return Collections.unmodifiableList(result) as List<Traversal.Admin<S, E>>
    }

    override fun modulateBy(selectTraversal: Traversal.Admin<*, *>?) {
        traversalRing.addTraversal(integrateChild(selectTraversal))
    }

    override fun toString(): String {
        return StringFactory.stepString(
            this,
            this.propertyKeys.toList(),
            traversalRing,
            propertyType.name.lowercase()
        )
    }

    override fun clone(): ChronoGraphPropertyMapStep<K, E> {
        val clone = super.clone() as ChronoGraphPropertyMapStep<K, E>
        if (null != propTraversal) clone.propTraversal = propTraversal!!.clone()
        clone.traversalRing = traversalRing.clone()
        return clone
    }

    override fun hashCode(): Int {
        var result = super.hashCode() xor propertyType.hashCode() xor Integer.hashCode(tokens)
        if (null != propTraversal) result = result xor propTraversal.hashCode()
        for (propertyKey in propertyKeys) {
            result = result xor propertyKey.hashCode()
        }
        return result xor traversalRing.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) {
            return true
        }
        if (other !is ChronoGraphPropertyMapStep<*, *>) {
            return false
        }
        if (other.getIncludedTokens() != this.getIncludedTokens()) {
            return false
        }
        if (other.propertyType != this.propertyType) {
            return false
        }
        if (!other.propertyKeys.contentEquals(this.propertyKeys)) {
            return false
        }
        return this.traversalRing == other.traversalRing
    }

    override fun setTraversal(parentTraversal: Traversal.Admin<*, *>?) {
        super.setTraversal(parentTraversal)
        if (null != propTraversal) integrateChild<Any, Any>(propTraversal)
        traversalRing.traversals.forEach { integrateChild<K, E>(it) }
    }

    override fun getRequirements(): Set<TraverserRequirement?>? {
        return getSelfAndChildRequirements(TraverserRequirement.OBJECT)
    }

    fun getIncludedTokens(): Int {
        return tokens
    }

    private fun includesToken(token: Int): Boolean {
        return 0 != tokens and token
    }

    private fun includesAnyTokenOf(vararg tokens: Int): Boolean {
        for (token in tokens) {
            if (this.includesToken(token)) {
                return true
            }
        }
        return false
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported here!")
    }

    private data class TypedID(
        val type: ElementType,
        val id: String,
    )

    private enum class ElementType {
        VERTEX,
        EDGE
    }
}