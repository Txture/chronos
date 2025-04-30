package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.PropertyType
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.structure.ChronoEdge
import org.chronos.chronograph.api.structure.ChronoElement
import org.chronos.chronograph.api.structure.ChronoVertex
import org.chronos.chronograph.api.structure.ElementLifecycleStatus
import org.chronos.chronograph.api.structure.ElementLifecycleStatus.PERSISTED
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.concurrent.Callable


class ChronoGraphPropertiesStep<E> : PropertiesStep<E>, Configuring, Prefetching {

    private val prefetcher: Prefetcher

    constructor(traversal: Traversal.Admin<*, *>, propertyType: PropertyType, propertyKeys: Array<out String>, labels: Set<String>)
        : super(traversal, propertyType, *propertyKeys) {
        this.labels.addAll(labels)
        this.prefetcher = Prefetcher(traversal, propertyKeys)
    }

    override fun registerFutureElementForPrefetching(element: Element) {
        this.prefetcher.registerFutureElementForPrefetching(element)
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported!")
    }

    @Suppress("UNCHECKED_CAST")
    override fun flatMap(traverser: Traverser.Admin<Element>): Iterator<E> {
        val element = traverser.get()
        if (element is ChronoElement && element.status != ElementLifecycleStatus.PERSISTED) {
            // the element is dirty (new or changed) so we can't use the secondary index.
            // Fall back to the default algorithm.
            return this.flatMapWithStandardAlgorithm(traverser)
        }

        val cachedQueryResult = this.prefetcher.getPrefetchResult(element)
            ?: return this.flatMapWithStandardAlgorithm(traverser) // the query couldn't be processed by the secondary index

        return this.propertyKeys.asSequence().flatMap { cachedQueryResult[it] ?: emptySet() }.iterator() as Iterator<E>
    }

    @Suppress("UNCHECKED_CAST")
    private fun flatMapWithStandardAlgorithm(traverser: Traverser.Admin<Element>): Iterator<E> {
        return when (this.returnType) {
            PropertyType.VALUE -> {
                val element = traverser.get()
                propertyKeys.asSequence().flatMap {
                    when (val value = element.property<Any>(it).orElse(null)) {
                        null -> sequenceOf()
                        is Collection<*> -> value.asSequence().distinct()
                        else -> sequenceOf(value)
                    }
                }.iterator() as Iterator<E>
            }

            PropertyType.PROPERTY -> {
                traverser.get().properties<E>(*propertyKeys) as Iterator<E>
            }

            null -> {
                // this actually can't happen because we're declaring the 'propertyType' constructor
                // variable as non-nullable. It's only nullable in theory because the base class is
                // a Java class.
                throw IllegalArgumentException("PropertiesStep.returnType is NULL!")
            }
        }
    }

}