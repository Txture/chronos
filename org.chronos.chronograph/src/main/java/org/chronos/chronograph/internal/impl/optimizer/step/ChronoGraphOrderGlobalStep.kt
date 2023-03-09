package org.chronos.chronograph.internal.impl.optimizer.step

import com.google.common.collect.HashBasedTable
import com.google.common.collect.Table
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.util.StringFactory
import org.apache.tinkerpop.gremlin.util.function.MultiComparator
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.largestComparable
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.smallestComparable
import org.chronos.chronograph.api.builder.query.ordering.COrder
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.optimizer.traversals.ElementValueOrNullTraversal
import org.chronos.chronograph.internal.impl.optimizer.traversals.TableLookupTraversal
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import org.javatuples.Pair
import java.util.*
import org.chronos.chronodb.api.Order as ChronoDBOrder

@Suppress("UNCHECKED_CAST")
class ChronoGraphOrderGlobalStep<S, C : Comparable<*>>
    : CollectingBarrierStep<S>,
    ComparatorHolder<S, C>,
    TraversalParent,
    ByModulating,
    Seedable {

    private val comparators: MutableList<Pair<Traversal.Admin<S, C>, Comparator<C>>> = ArrayList<Pair<Traversal.Admin<S, C>, Comparator<C>>>()
    private var multiComparator: MultiComparator<C>? = null
    private var limit = Long.MAX_VALUE
    private val random: Random

    private var cacheTable: Table<String, String, Comparable<C>>? = null

    constructor(
        traversal: Traversal.Admin<*, *>,
        comparators: List<Pair<Traversal.Admin<S, C>, Comparator<C>>>,
        limit: Long,
        random: Random,
        labels: Set<String>
    ) : super(traversal) {
        for(comparator in comparators){
            this.addComparator(comparator.value0, comparator.value1)
        }
        this.limit = limit
        this.random = random
        this.labels.addAll(labels)
    }

    override fun resetSeed(seed: Long) {
        random.setSeed(seed)
    }

    override fun barrierConsumer(traverserSet: TraverserSet<S>) {
        if (this.multiComparator == null) {
            this.multiComparator = createMultiComparator()
        }
        if (multiComparator!!.isShuffle) {
            traverserSet.shuffle(random)
        } else {
            traverserSet.sort(multiComparator as Comparator<Traverser<S>>)
        }
    }

    override fun processAllStarts() {
        val traversal = getTraversal<Any, Any>()
        val allTraversers = this.starts.asSequence().toList()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal )
        val graph = ChronoGraphTraversalUtil.getChronoGraph(traversal)
        val indexManager = graph.getIndexManagerOnBranch(tx.branchName) as ChronoGraphIndexManagerInternal
        indexManager.withIndexReadLock {
            this.cacheTable = this.createCacheTable(allTraversers)
            for (traverser in allTraversers) {
                val projectedTraverser = createProjectedTraverser(traverser)
                this.traverserSet.add(projectedTraverser)
            }
        }
    }

    private fun createCacheTable(traversers: Iterable<Traverser<S>>): Table<String, String, Comparable<C>>? {
        if (this.comparators.isEmpty()) {
            // no comparators given... can't use index sort
            return null
        }
        val graph = this.traversal.graph.orElse(null) as ChronoGraphInternal
        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(getTraversal<Any, Any>())
        if (tx.context.isDirty) {
            // can't use index queries on dirty context
            return null
        }

        val elements = traversers.asSequence().map { it.get() }.toList()
        val elementTypes = mutableSetOf<Class<*>>()
        val primaryKeys = mutableSetOf<String>()
        for (element in elements) {
            if (element !is ChronoElement) {
                // we're dealing with an unknown element -> can't use indexed sort
                return null
            }
            elementTypes += when (element) {
                is Vertex -> Vertex::class.java
                is Edge -> Edge::class.java
                else -> {
                    // we're dealing with an unknown element -> can't use indexed sort
                    return null
                }
            }
            if (!element.isLazy) {
                // the element is not lazy-loaded, i.e. we already have it
                // fully in-memory. We gain nothing by fetching its data from
                // the secondary index again. It would make our index query slower.
                continue
            }
            primaryKeys += element.id() as String

        }

        if (elementTypes.size != 1) {
            // there are multiple types in the input -> can't use index sort
            return null
        }

        val branch = graph.backingDB.branchManager.getBranch(tx.branchName)

        if (!this.canUseSecondaryIndex()) {
            return null
        }

        val elementType = elementTypes.single()
        val (keyspace, indices) = when (elementType) {
            Vertex::class.java -> {
                val keyspace = ChronoGraphConstants.KEYSPACE_VERTEX
                val indices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedVertexPropertiesAtTimestamp(tx.timestamp)
                keyspace to indices
            }
            Edge::class.java -> {
                val keyspace = ChronoGraphConstants.KEYSPACE_EDGE
                val indices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedEdgePropertiesAtTimestamp(tx.timestamp)
                keyspace to indices
            }
            else -> {
                // unknown type, can't use index sort
                return null
            }
        }

        val graphPropertyKeyToIndex = indices.associateBy { it.indexedProperty }
        val allPropertiesIndexed = this.comparators.all { entry ->
            val traversal = entry.value0
            val comparator = entry.value1
            if (comparator !is COrder && comparator != Order.asc && comparator != Order.desc) {
                // don't touch this one
                return@all false
            }
            when {
                traversal is ElementValueOrNullTraversal<*> -> {
                    val graphPropertyKey = traversal.propertyKey
                    val index = graphPropertyKeyToIndex[graphPropertyKey]
                    if (index as? ChronoGraphIndexInternal? != null) {
                        return@all true
                    }
                }
                traversal is ValueTraversal<*, *> -> {
                    val graphPropertyKey = traversal.propertyKey
                    val index = graphPropertyKeyToIndex[graphPropertyKey]
                    if (index as? ChronoGraphIndexInternal? != null) {
                        return@all true
                    }
                }
                traversal is TokenTraversal && traversal.token == T.id -> {
                    return@all true
                }
            }
            return@all false
        }

        if (!allPropertiesIndexed) {
            return null
        }

        val indexValuesTable = HashBasedTable.create<String, String, Comparable<C>>(primaryKeys.size, comparators.size)

        for (entry in this.comparators) {
            val traversal = entry.value0
            val comparator = entry.value1
            val order = comparator.toChronoDBSortOrder()!!

            val graphPropertyKey = when (traversal) {
                is ElementValueOrNullTraversal<*> -> traversal.propertyKey
                is ValueTraversal<*, *> -> traversal.propertyKey
                else -> continue
            }

            val index = graphPropertyKeyToIndex.getValue(graphPropertyKey) as ChronoGraphIndexInternal
            val primaryKeyToIndexValues = graph.backingDB.indexManager.getIndexedValuesByKey(
                tx.timestamp,
                branch,
                keyspace,
                index.backendIndexKey,
                primaryKeys
            )
            for ((primaryKey, indexValues) in primaryKeyToIndexValues) {
                val indexValue = when (order) {
                    ChronoDBOrder.ASCENDING -> indexValues.smallestComparable()
                    ChronoDBOrder.DESCENDING -> indexValues.largestComparable()
                }
                indexValuesTable.put(primaryKey, graphPropertyKey, indexValue as Comparable<C>)
            }
        }
        return indexValuesTable
    }

    private fun canUseSecondaryIndex(): Boolean {
        val canUseIndex = this.comparators.all { entry ->
            val traversal = entry.value0
            val comparator = entry.value1
            if (comparator !is COrder && comparator != Order.asc && comparator != Order.desc) {
                // don't touch this one
                return@all false
            }
            if (traversal is ValueTraversal<*, *>) {
                // element value traversals are okay, they extract a single
                // property value.
                return@all true
            }
            if (traversal is ElementValueOrNullTraversal<*>) {
                // element value traversals are okay, they extract a single
                // property value.
                return@all true
            }
            if (traversal is TokenTraversal && traversal.token == T.id) {
                // ID is okay for indexing, all other tokens are not.
                return@all true
            }
            return@all false
        }
        return canUseIndex
    }

    private fun Comparator<*>.toChronoDBSortOrder(): ChronoDBOrder? {
        return when (this) {
            Order.asc -> ChronoDBOrder.ASCENDING
            Order.desc -> ChronoDBOrder.DESCENDING
            is COrder -> this.direction
            else -> null
        }
    }

    override fun addComparator(traversal: Traversal.Admin<S, C>, comparator: Comparator<C>) {
        comparators.add(Pair(integrateChild(traversal), this.replaceGremlinComparator(comparator)))
    }

    private fun replaceGremlinComparator(comparator: Comparator<C>): Comparator<C> {
        return when (comparator) {
            Order.asc -> COrder.asc() as Comparator<C>
            Order.desc -> COrder.desc() as Comparator<C>
            else -> comparator // custom comparator
        }
    }

    override fun modulateBy(traversal: Traversal.Admin<*, *>) {
        this.modulateBy(traversal, Order.asc)
    }

    override fun modulateBy(traversal: Traversal.Admin<*, *>, comparator: Comparator<*>) {
        addComparator(traversal as Traversal.Admin<S, C>, comparator as Comparator<C>)
    }

    override fun replaceLocalChild(oldTraversal: Traversal.Admin<*, *>, newTraversal: Traversal.Admin<*, *>?) {
        var i = 0
        for (pair in comparators) {
            val traversal = pair.value0
            if (null != traversal && traversal == oldTraversal) {
                comparators[i] = Pair.with(this.integrateChild(newTraversal), pair.value1)
                break
            }
            i++
        }
    }

    override fun getComparators(): List<Pair<Traversal.Admin<S, C>, Comparator<C>>> {
        return if (comparators.isEmpty()) {
            listOf(Pair<Any, Any>(IdentityTraversal<Any>(), Order.asc as Comparator<*>)) as List<Pair<Traversal.Admin<S, C>, Comparator<C>>>
        } else {
            Collections.unmodifiableList(comparators)
        }
    }

    override fun toString(): String {
        return StringFactory.stepString(this, comparators)
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        for (i in comparators.indices) {
            result = result xor comparators[i].hashCode() * (i + 1)
        }
        return result
    }

    override fun getRequirements(): Set<TraverserRequirement?>? {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.OBJECT)
    }

    override fun <S : Any?, E : Any?> getLocalChildren(): List<Traversal.Admin<S, E>> {
        return comparators.map { it.value0 } as List<Traversal.Admin<S, E>>
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported here!")
    }

    override fun clone(): ChronoGraphOrderGlobalStep<S, C> {
        val clone = super.clone() as ChronoGraphOrderGlobalStep<S, C>
        for (comparator in comparators) {
            clone.comparators.add(Pair(comparator.value0.clone(), comparator.value1))
        }
        return clone
    }

    override fun setTraversal(parentTraversal: Traversal.Admin<*, *>) {
        super.setTraversal(parentTraversal)
        comparators.asSequence()
            .map { it.value0 }
            .forEach { this.integrateChild<S, C>(it) }
    }

    override fun getMemoryComputeKey(): MemoryComputeKey<TraverserSet<S>> {
        if (null == multiComparator) multiComparator = createMultiComparator()
        return MemoryComputeKey.of(this.getId(), OrderGlobalStep.OrderBiOperator(limit, multiComparator, random), false, true)
    }

    private fun createProjectedTraverser(traverser: Traverser.Admin<S>): ProjectedTraverser<S, Any> {
        val projections: MutableList<Any?> = ArrayList(this.comparators.size)
        for (pair in this.comparators) {
            val traversal = pair.value0
            val comparator = pair.value1
            val replacedTraversal = if (this.cacheTable != null && traversal is ValueTraversal<*,*>) {
                TableLookupTraversal(this.cacheTable, traversal.propertyKey, comparator as Comparator<Comparable<C>>)
            } else if (traversal is ValueTraversal<*,*>) {
                ElementValueOrNullTraversal<Any>(traversal.propertyKey) as Traversal.Admin<S, C>
            } else {
                traversal
            }
            projections.add(TraversalUtil.apply(traverser, replacedTraversal))
        }
        return ProjectedTraverser(traverser, projections)
    }

    private fun createMultiComparator(): MultiComparator<C> {
        val list: MutableList<Comparator<C>> = ArrayList<Comparator<C>>(comparators.size)
        for (pair in comparators) {
            list.add(pair.value1)
        }
        return MultiComparator(list)
    }

}