package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.PropertyType
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.api.structure.ElementLifecycleStatus
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil


class ChronoGraphPropertiesStep<E> : PropertiesStep<E>, Configuring {

    private var initialized = false
    private var indexQueryResult: Map<String, Set<Comparable<*>>>? = null

    constructor(traversal: Traversal.Admin<*, *>, propertyType: PropertyType, propertyKeys: Array<out String>, labels: Set<String>)
        : super(traversal, propertyType, *propertyKeys) {
        this.labels.addAll(labels)
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported!")
    }

    override fun processNextStart(): Traverser.Admin<E> {
        this.initialize()
        return super.processNextStart()
    }

    private fun initialize() {
        if (initialized) {
            return
        }
        initialized = true
        // collect all the elements in the current traversal,
        // execute the query, and cache its result.
        if (this.returnType != PropertyType.VALUE) {
            // we can only return values from the index query -> not applicable.
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
    override fun flatMap(traverser: Traverser.Admin<Element>): Iterator<E> {
        val element = traverser.get()
        if (element is ChronoElement && element.status != ElementLifecycleStatus.PERSISTED) {
            // the element is dirty (new or changed) so we can't use the secondary index.
            // Fall back to the default algorithm.
            return this.flatMapWithStandardAlgorithm(traverser)
        }

        val cachedQueryResult = this.indexQueryResult
            ?: return this.flatMapWithStandardAlgorithm(traverser) // the query couldn't be processed by the secondary index

        val primaryKey = traverser.get().id() as String
        val cachedResult = cachedQueryResult[primaryKey]
        return if (cachedResult.isNullOrEmpty()) {
            emptySet<E>().iterator()
        } else {
            cachedResult.iterator() as Iterator<E>
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun flatMapWithStandardAlgorithm(traverser: Traverser.Admin<Element>): Iterator<E> {
        return if (returnType == PropertyType.VALUE) {
            val element = traverser.get()
            propertyKeys.asSequence().flatMap {
                when (val value = element.property<Any>(it).orElse(null)) {
                    null -> sequenceOf()
                    is Collection<*> -> value.asSequence().distinct()
                    else -> sequenceOf(value)
                }
            }.iterator() as Iterator<E>
        } else {
            traverser.get().properties<E>(*propertyKeys) as Iterator<E>
        }
    }

    private fun getValuesByIndexScan(traversers: List<Traverser.Admin<Element>>): Map<String, Set<Comparable<*>>>? {
        val (indices, keyspace) = ChronoGraphStepUtil.getIndicesAndKeyspace(traversal, traversers, this.propertyKeys.toSet())
            ?: return null
        val primaryKeys = traversers.asSequence()
            .map { it.get() }
            // no need to query the secondary index for dirty elements
            .filter { (it is ChronoElement) && it.status == ElementLifecycleStatus.PERSISTED }
            .map { it.id() as String }
            .toSet()
        val propertyKeysAsSet = this.propertyKeys.toSet()
        val chronoDBPropertyKeys = indices.asSequence()
            .filter { it.indexedProperty in propertyKeysAsSet }
            .map { (it as ChronoGraphIndexInternal).backendIndexKey }
            .toSet()
        val graph = this.traversal.graph.orElse(null) as ChronoGraphInternal
        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(getTraversal<Any, Any>())
        val branch = graph.backingDB.branchManager.getBranch(tx.branchName)
        val resultMap = mutableMapOf<String, MutableSet<Comparable<*>>>()
        for (chronoDBPropertyKey in chronoDBPropertyKeys) {
            val indexScanResult = graph.backingDB.indexManager.getIndexedValuesByKey(
                tx.timestamp,
                branch,
                keyspace,
                chronoDBPropertyKey,
                primaryKeys
            )
            for ((primaryKey, indexValues) in indexScanResult) {
                resultMap.getOrPut(primaryKey, ::mutableSetOf).addAll(indexValues)
            }
        }
        return resultMap
    }


}