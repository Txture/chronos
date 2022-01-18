package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.chronos.chronograph.api.structure.ChronoGraph

abstract class ChronoGraphStrategy : AbstractTraversalStrategy<ProviderOptimizationStrategy>(), ProviderOptimizationStrategy {

    protected val Traversal.Admin<*, *>.chronoGraph: ChronoGraph?
        get() {
            val g = this.graph.orElse(null)
            if (g is ChronoGraph) {
                return g
            }
            val parent = this.parent
            if (parent != null && parent is Traversal.Admin<*, *>) {
                return parent.chronoGraph
            }
            return null
        }

}