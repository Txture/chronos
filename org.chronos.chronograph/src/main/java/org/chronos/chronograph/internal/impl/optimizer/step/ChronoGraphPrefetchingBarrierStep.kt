package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet
import org.apache.tinkerpop.gremlin.structure.Element

class ChronoGraphPrefetchingBarrierStep<S> : CollectingBarrierStep<S>, Prefetching {

    val clients: MutableSet<Prefetching>

    constructor(traversal: Admin<*, *>, clients: List<Prefetching>) : this(traversal, Int.MAX_VALUE, clients)

    constructor(traversal: Admin<*, *>, vararg clients: Prefetching) : this(traversal, Int.MAX_VALUE, clients.asList())

    constructor(traversal: Admin<*, *>, maxBarrierSize: Int, vararg clients: Prefetching) : this(traversal, maxBarrierSize, clients.asList())


    constructor(traversal: Admin<*, *>, maxBarrierSize: Int, clients: List<Prefetching>) : super(traversal, maxBarrierSize) {
        this.clients = clients.toMutableSet()
    }

    fun addClients(prefetchingStepsInChildTraversals: Iterable<Prefetching>) {
        this.clients.addAll(prefetchingStepsInChildTraversals)
    }

    override fun barrierConsumer(traverserSet: TraverserSet<S>) {
        for (traverser in traverserSet) {
            val element = traverser.get()
            if (element !is Element) {
                // we're interested only in graph elements.
                continue
            }
            for (client in this.clients) {
                // tell the client that this element is about to come.
                client.registerFutureElementForPrefetching(element)
            }
        }
    }

    override fun remove() {
        // gremlin query steps implement Iterator, but they're read-only.
        throw UnsupportedOperationException("remove() is not supported!")
    }

    override fun registerFutureElementForPrefetching(element: Element) {
        // the prefetching barrier itself is prefetching. This allows us to
        // receive elements for prefetching from parent traversals. All we
        // need to do is to redistribute them to our children.
        for(client in this.clients){
            client.registerFutureElementForPrefetching(element)
        }
    }

    override fun toString(): String {
        return "ChronoGraphPrefetchingBarrierStep(${this.clients.size} clients)"
    }

}