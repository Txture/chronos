package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.structure.ChronoEdge
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.api.structure.ChronoVertex
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.concurrent.Callable
import kotlin.math.max

class Prefetcher : Prefetching {

    companion object {

        /**
         * How many elements need to be in the prefetch set for us to perform an index query?
         *
         * If there are fewer elements in the prefetch set than indicated by this setting, we
         * will not perform an index query at all and evaluate them one-by-one instead.
         */
        const val DEFAULT_PREFETCH_INDEX_QUERY_MIN_ELEMENTS = 100

    }

    private val traversal: Traversal.Admin<*, *>
    private val prefetchIndexQueryMinElements: Int

    private var isPrefetchCheckRequired = false
    private val verticesToPrefetch = mutableSetOf<Vertex>()
    private val edgesToPrefetch = mutableSetOf<Edge>()
    private val propertyKeys = mutableSetOf<String>()
    private val customConditions = mutableListOf<() -> Boolean>()

    constructor(traversal: Traversal.Admin<*, *>, propertyKeys: Iterable<String> = emptyList()) {
        this.traversal = traversal
        this.prefetchIndexQueryMinElements = traversal.prefetchIndexQueryMinElementsSetting
        this.setPropertyKeys(propertyKeys)
    }

    private val Traversal.Admin<*, *>.prefetchIndexQueryMinElementsSetting: Int
        get() {
            val chronoGraph = graph.orElse(null) as? ChronoGraph?
            // find the minimum number of elements which are required for an index prefetch query.
            val options = strategies.getStrategy(OptionsStrategy::class.java).orElse(null)?.options
            // first, consult the options of the traversal itself.
            val value = options?.get(ChronoGraphConfiguration.PREFETCH_INDEX_QUERY_MIN_ELEMENTS) as? Int?
            // fallback: check the chronograph configuration
                ?: chronoGraph?.chronoGraphConfiguration?.minimumNumberOfElementsForPrefetchIndexQuery
                // fallback: default
                ?: DEFAULT_PREFETCH_INDEX_QUERY_MIN_ELEMENTS
            // don't allow negative values
            return max(0, value)
        }

    constructor(traversal: Traversal.Admin<*, *>, propertyKeys: Array<out String>) : this(traversal, propertyKeys.asList())

    /**
     * The cached query result.
     *
     * The parameters are:
     * - outer map key (string): the primary key of the vertex/edge
     * - inner map key (string): The name of the property
     * - cell value (set<comparable>): the indexed values.
     */
    private var indexQueryResult: MutableMap<TypedID, MutableMap<String, MutableSet<Comparable<*>>>>? = null

    fun setPropertyKeys(propertyKeys: Iterable<String>) {
        this.propertyKeys.clear()
        this.propertyKeys.addAll(propertyKeys)
    }

    fun addCustomPrefetchingCondition(condition: () -> Boolean) {
        this.customConditions += condition
    }

    override fun registerFutureElementForPrefetching(element: Element) {
        if (this.hasPrefetchedIndexResultFor(element)) {
            // we've already fetched the row for this element,
            // no need to prefetch twice.
            return
        }
        val changed = when (element) {
            is Vertex -> {
                verticesToPrefetch.add(element)
            }

            is Edge -> {
                edgesToPrefetch.add(element)
            }

            else -> {
                // we don't care
                return
            }
        }
        if (changed) {
            // we'll need to reconsider our prefetching condition
            this.isPrefetchCheckRequired = true
        }
    }

    private fun hasPrefetchedIndexResultFor(element: Element): Boolean {
        val table = this.indexQueryResult
            ?: return false // we haven't done any prefetching yet

        val typedId = element.typedID
        return table.containsKey(typedId)
    }

    private fun updatePrefetchedIndexIfNecessary() {
        if (!shouldPerformPrefetch()) {
            return
        }
        // fully-loaded vertices and edges don't require prefetching, we're interested only in the lazy-loading ones.
        val lazyVertices = this.verticesToPrefetch.asSequence()
            .filterIsInstance<ChronoVertex>()
            .filter(ChronoVertex::isLazy)
            .toSet()

        val lazyEdges = this.edgesToPrefetch.asSequence()
            .filterIsInstance<ChronoEdge>()
            .filter(ChronoEdge::isLazy)
            .toSet()

        if ((lazyVertices.size + lazyEdges.size) < this.prefetchIndexQueryMinElements) {
            // too few elements in the prefetch set; index query isn't worth it, it's faster to fetch them one-by-one.
            return
        }

        val newIndexResults = this.getValuesByIndexScan(lazyVertices, lazyEdges)
        if (newIndexResults != null) {
            val indexQueryResult = this.indexQueryResult
            if (indexQueryResult == null) {
                // this is the first index query we're doing, assign directly
                this.indexQueryResult = newIndexResults
            } else {
                // we've already done at least one index query; merge with
                // the previous result.
                indexQueryResult.putAll(newIndexResults)
            }
        }

        // after we've done the index query, discard the prefetched elements (they're now in the indexQueryResult)
        this.verticesToPrefetch.clear()
        this.edgesToPrefetch.clear()
    }

    private fun shouldPerformPrefetch(): Boolean {
        if (!isPrefetchCheckRequired) {
            // nothing has changed since last time, so there's nothing new to prefetch.
            return false
        }

        // don't perform this check next time, unless the prefetch set size changed
        isPrefetchCheckRequired = false
        if (this.propertyKeys.isEmpty()) {
            // no optimization possible if user requests ALL properties. Fall back to standard algorithm.
            return false
        }
        if (this.propertyKeys.size > 5) {
            // with too many required keys (i.e. the projection is "too wide"), it is likely faster to just load
            // the graph elements and query them directly.
            return false
        }
        if ((verticesToPrefetch.size + edgesToPrefetch.size) < this.prefetchIndexQueryMinElements) {
            // too few elements in the prefetch set; index query isn't worth it, it's faster to fetch them one-by-one.
            return false
        }

        // fully-loaded vertices and edges don't require prefetching, we're interested only in the lazy-loading ones.
        val lazyVertices = this.verticesToPrefetch.asSequence()
            .filterIsInstance<ChronoVertex>()
            .filter(ChronoVertex::isLazy)
            .toSet()

        val lazyEdges = this.edgesToPrefetch.asSequence()
            .filterIsInstance<ChronoEdge>()
            .filter(ChronoEdge::isLazy)
            .toSet()

        if ((lazyVertices.size + lazyEdges.size) < this.prefetchIndexQueryMinElements) {
            // too few elements in the prefetch set; index query isn't worth it, it's faster to fetch them one-by-one.
            return false
        }
        if (this.customConditions.any { !it() }) {
            // custom condition evaluated to false
            return false
        }
        return true
    }

    fun getPrefetchResult(element: Element): Map<String, Set<Comparable<*>>>? {
        if (element !is ChronoElement) {
            // this isn't a ChronoElement, so it doesn't support
            // lazy loading -> don't attempt to use the secondary
            // index, use the element itself.
            return null
        }
        if (!element.isLazy) {
            // element is loaded in main-memory, use that instead.
            return null
        }

        // make sure that we have done all required prefetching
        this.updatePrefetchedIndexIfNecessary()
        val table = this.indexQueryResult
            ?: return null // the entire table is missing

        return table[element.typedID]
    }

    private val Element.typedID: TypedID
        get() {
            val primaryKey = this.id() as String
            val elementType = when (this) {
                is Vertex -> ElementType.VERTEX
                is Edge -> ElementType.EDGE
                else -> throw IllegalArgumentException("Unknown type of Element: '${this::class.qualifiedName}'!")
            }
            return TypedID(elementType, primaryKey)
        }

    private fun getValuesByIndexScan(vertices: Set<Vertex>, edges: Set<Edge>): MutableMap<TypedID, MutableMap<String, MutableSet<Comparable<*>>>>? {
        val chronoGraph = ChronoGraphTraversalUtil.getChronoGraph(traversal) as ChronoGraphInternal
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)
        val indexManager = chronoGraph.getIndexManagerOnBranch(tx.branchName) as ChronoGraphIndexManagerInternal
        val requestedProperties = this.propertyKeys.toSet()
        return indexManager.withIndexReadLock(Callable {
            val resultTable = mutableMapOf<TypedID, MutableMap<String, MutableSet<Comparable<*>>>>()
            if (vertices.isNotEmpty()) {
                performIndexScanAndAddToResultTable(tx, requestedProperties, chronoGraph, vertices, resultTable)
            }
            if (edges.isNotEmpty()) {
                performIndexScanAndAddToResultTable(tx, requestedProperties, chronoGraph, edges, resultTable)
            }

            return@Callable resultTable
        })
    }

    private inline fun <reified T : Element> performIndexScanAndAddToResultTable(
        tx: ChronoGraphTransaction,
        requestedProperties: Set<String>,
        chronoGraph: ChronoGraphInternal,
        elements: Set<T>,
        resultTable: MutableMap<TypedID, MutableMap<String, MutableSet<Comparable<*>>>>,
    ) {
        // we can only proceed if ALL requested properties are indexed on the elements. If there is just
        // a single requested property which isn't indexed, we will need to fetch the elements anyway -> no need for prefetching.
        if (!ChronoGraphStepUtil.areAllPropertiesIndexed(tx, T::class.java, requestedProperties)) {
            return
        }

        val elementType = when (T::class) {
            Vertex::class, ChronoVertex::class -> ElementType.VERTEX
            Edge::class, ChronoEdge::class -> ElementType.EDGE
            else -> throw IllegalArgumentException("Unknown subclass of Element: '${T::class.qualifiedName}'")
        }

        val indices = when (elementType) {
            ElementType.VERTEX -> chronoGraph.getIndexManagerOnBranch(tx.branchName).getIndexedVertexPropertiesAtTimestamp(tx.timestamp)
            ElementType.EDGE -> chronoGraph.getIndexManagerOnBranch(tx.branchName).getIndexedEdgePropertiesAtTimestamp(tx.timestamp)
        }
        val primaryKeys = elements.asSequence()
            .map { it.id() as String }
            .toSet()
        val branch = chronoGraph.backingDB.branchManager.getBranch(tx.branchName)
        for (index in indices) {
            if (index.indexedProperty !in requestedProperties) {
                // we're not interested in the results of this index.
                continue
            }
            val propertyName = index.indexedProperty
            val chronoDBPropertyName = (index as ChronoGraphIndexInternal).backendIndexKey
            val keyspace = when (elementType) {
                ElementType.VERTEX -> ChronoGraphConstants.KEYSPACE_VERTEX
                ElementType.EDGE -> ChronoGraphConstants.KEYSPACE_EDGE
            }
            val indexScanResult = chronoGraph.backingDB.indexManager.getIndexedValuesByKey(
                tx.timestamp,
                branch,
                keyspace,
                chronoDBPropertyName,
                primaryKeys
            )

            // ensure that every element has a row in the table (even if it's empty)
            // this allows us later to rely on the results of the table, even if the
            // secondary index didn't have any value for the graph elements because
            // they were lacking the properties.
            for (element in elements) {
                resultTable.getOrPut(TypedID(elementType, element.id() as String), ::mutableMapOf)
            }
            // fill the rows with the results from the secondary index scan
            for ((primaryKey, indexValues) in indexScanResult) {
                val row = resultTable.getOrPut(TypedID(elementType, primaryKey), ::mutableMapOf)
                val cellContent = row.getOrPut(propertyName, ::mutableSetOf)
                cellContent.addAll(indexValues)
            }
        }
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