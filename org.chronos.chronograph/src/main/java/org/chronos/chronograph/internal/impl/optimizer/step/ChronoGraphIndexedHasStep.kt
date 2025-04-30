package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronograph.api.structure.ChronoElement

class ChronoGraphIndexedHasStep<S : Element> : HasStep<S>, Prefetching {

    private val prefetcher: Prefetcher

    constructor(traversal: Traversal.Admin<*, *>, vararg hasContainers: HasContainer) :
        super(traversal, *hasContainers) {
        val propertyKeys = this.hasContainers.map { it.key }
        this.prefetcher = Prefetcher(this.traversal, propertyKeys)
    }

    override fun remove() {
        throw UnsupportedOperationException("This iterator is immutable!")
    }

    override fun registerFutureElementForPrefetching(element: Element) {
        this.prefetcher.registerFutureElementForPrefetching(element)
    }

    override fun filter(traverser: Traverser.Admin<S>): Boolean {
        val element = traverser.get()
        return when (element) {
            is Property<*> -> super.filter(traverser)
            is Element -> testElement(element)
            else -> super.filter(traverser)
        }
    }

    private fun testElement(element: Element): Boolean {
        if (element is ChronoElement) {
            if (!element.isLazy) {
                // the element has already been loaded from disk,
                // use the regular semantics
                return HasContainer.testAll(element, this.hasContainers)
            }
        }
        // we're dealing with a transient element. Rely on the prefetcher.
        val prefetchedProperties = this.prefetcher.getPrefetchResult(element)
        for (hasContainer in this.hasContainers) {
            if (!testSingleHasContainer(element, prefetchedProperties, hasContainer)) {
                // one of the "has" clauses failed -> the entire check fails.
                return false
            }
        }
        // all checks succeeded.
        return true
    }

    @Suppress("UNCHECKED_CAST")
    private fun testSingleHasContainer(
        element: Element,
        prefetchedProperties: Map<String, Set<Comparable<*>>>?,
        hasContainer: HasContainer,
    ): Boolean {
        if (hasContainer.key == T.id.accessor) {
            // checks on IDs are also possible on lazy elements without loading them.
            return hasContainer.test(element)
        }

        if (prefetchedProperties == null) {
            // prefetcher didn't do anything for this element. This can mean that
            // we didn't reach "critical mass" to make prefetching worth it, or
            // the element is already loaded. Run the regular algorithm.
            return hasContainer.test(element)
        }

        // note that the value may be NULL here. That's totally fine, in this case
        // the secondary index told us that the element doesn't have the property at all.
        val propertyValue = prefetchedProperties[hasContainer.key]

        return if (propertyValue != null) {
            // we got a value from the secondary index; test it and DO NOT load the element!
            // (this is the critical case where we actually gain performance!)
            (hasContainer.predicate as P<Any>).test(propertyValue)
        } else {
            // there is no value for the property, at all. Most likely, the property doesn't even
            // exist on the element. ANY "has(...)" clause will fail in this case, no matter what
            // the clause is.
            false
        }
    }

}