package org.chronos.chronograph.internal.impl.optimizer.step

import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.*
import java.util.concurrent.Callable

class ChronoGraphPropertyMapStep<K, E> : ScalarMapStep<Element, MutableMap<K, E>>, TraversalParent, ByModulating, Configuring {

    val propertyKeys: Array<out String>
    val propertyType: PropertyType

    private var tokens = 0
    private var propTraversal: Traversal.Admin<Element, out Property<*>>? = null

    private val parameters = Parameters()
    private var traversalRing: TraversalRing<K, E>

    private var initialized = false

    /**
     * The cached query result.
     *
     * The parameters are:
     * - row key (string): the primary key of the vertex/edge
     * - column key (string): The name of the property
     * - cell value (set<comparable>): the indexed values.
     */
    private var indexQueryResult: Table<String, String, Set<Comparable<*>>>? = null


    constructor(traversal: Traversal.Admin<*, *>?, propertyType: PropertyType, includedTokens: Int, propertyKeys: Array<out String>, labels: Set<String>) : super(traversal) {
        this.propertyKeys = propertyKeys
        this.propertyType = propertyType
        this.tokens = includedTokens
        this.labels.addAll(labels)
        propTraversal = null
        traversalRing = TraversalRing()
    }

    override fun processNextStart(): Traverser.Admin<MutableMap<K, E>> {
        this.initialize()
        return super.processNextStart()
    }

    private fun initialize() {
        if (this.initialized) {
            return
        }
        this.initialized = true
        if (this.includesAnyTokenOf(WithOptions.labels, WithOptions.keys, WithOptions.list)) {
            // option not supported in indexed mode, fall back to standard algorithm.
            return
        }
        if (this.propertyKeys.isEmpty()) {
            // no optimization possible if user requests ALL properties. Fall back to standard algorithm.
            return
        }
        if(this.propertyKeys.size > 5){
            // with too many required keys (i.e. the projection is "too wide"), it is likely faster to just load
            // the graph elements and query them directly.
            return
        }
        if (!starts.hasNext()) throw FastNoSuchElementException.instance()
        val elements = mutableListOf<Traverser.Admin<Element>>()
        // this will consume the elements -> the flatMap later won't work...
        starts.forEachRemaining(elements::add)
        // ... so we feed the elements back into the start
        starts.add(elements.iterator())

        // attempt to run the index query
        this.indexQueryResult = this.getValuesByIndexScan(elements)
    }

    @Suppress("UNCHECKED_CAST")
    private fun getValuesByIndexScan(traversers: List<Traverser.Admin<Element>>): Table<String, String, Set<Comparable<*>>>? {
        val chronoGraph = ChronoGraphTraversalUtil.getChronoGraph(traversal)
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)
        val indexManager = chronoGraph.getIndexManagerOnBranch(tx.branchName) as ChronoGraphIndexManagerInternal
        return indexManager.withIndexReadLock(Callable {
            val (indices, keyspace) = ChronoGraphStepUtil.getIndicesAndKeyspace(traversal, traversers, this.propertyKeys.toSet())
                ?: return@Callable null // unsupported setting

            val primaryKeys = traversers.asSequence()
                .map { it.get() }
                .map { it.id() as String }
                .toSet()
            val propertyKeysAsSet = this.propertyKeys.toSet()
            val graph = this.traversal.graph.orElse(null) as ChronoGraphInternal
            graph.tx().readWrite()
            val branch = graph.backingDB.branchManager.getBranch(tx.branchName)

            val resultTable = HashBasedTable.create<String, String, MutableSet<Comparable<*>>>()
            for (index in indices) {
                if (index.indexedProperty !in propertyKeysAsSet) {
                    continue
                }
                val propertyName = index.indexedProperty
                val chronoDBPropertyName = (index as ChronoGraphIndexInternal).backendIndexKey
                val indexScanResult = graph.backingDB.indexManager.getIndexedValuesByKey(
                    tx.timestamp,
                    branch,
                    keyspace,
                    chronoDBPropertyName,
                    primaryKeys
                )
                for ((primaryKey, indexValues) in indexScanResult) {
                    val row = resultTable.row(primaryKey)
                    val cellContent = row.getOrPut(propertyName, ::mutableSetOf)
                    cellContent.addAll(indexValues)
                }
            }

            return@Callable resultTable as Table<String, String, Set<Comparable<*>>>
        })
    }

    @Suppress("UNCHECKED_CAST")
    override fun map(traverser: Traverser.Admin<Element>): MutableMap<K, E> {
        val element = traverser.get()
        if (element is ChronoElement && !element.isLazy) {
            // the element is new in this transaction, or modified, or...
            // in any case, it is not "clean" and therefore the secondary index is useless.
            return this.mapWithStandardAlgorithm(traverser)
        }

        val cachedQueryResult = this.indexQueryResult
            ?: return this.mapWithStandardAlgorithm(traverser) // the query couldn't be processed by the secondary index

        val primaryKey = traverser.get().id() as String
        val cacheResultRow = cachedQueryResult.row(primaryKey)
        val map = if (cacheResultRow.isNullOrEmpty()) {
            mutableMapOf()
        } else {
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
}