package org.chronos.chronograph.internal.impl.optimizer.strategy

import com.google.common.collect.Sets
import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.index.ChronoGraphIndex
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphStep
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil

object ChronoGraphStepStrategy : ChronoGraphStrategy() {

    override fun applyPrior(): Set<Class<out ProviderOptimizationStrategy>> {
        val resultSet: MutableSet<Class<out ProviderOptimizationStrategy>> = Sets.newHashSet()
        resultSet.add(PredicateNormalizationStrategy::class.java)
        resultSet.add(ReplaceGremlinPredicateWithChronosPredicateStrategy::class.java)
        resultSet.add(OrderFiltersStrategy::class.java)
        return resultSet
    }

    // =====================================================================================================================
    // TINKERPOP API
    // =====================================================================================================================

    @Suppress("UNCHECKED_CAST")
    override fun apply(traversal: Traversal.Admin<*, *>) {
        // we do not use the helper method "traversal.chronoGraph" on purpose
        // here because we only want to apply this strategy on the ROOT traversal.
        val graph = traversal.graph.orElse(null)
        if (graph !is ChronoGraph) {
            // cannot apply traversal strategy...
            return
        }
        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)
        val indexManager = graph.getIndexManagerOnBranch(tx.branchName) as ChronoGraphIndexManagerInternal
        // we have a critical section here: until we're done analyzing the query and reading from the
        // secondary index, there must not be any concurrent modifications to the indices.
        indexManager.withIndexReadLock {
            val cleanIndices = indexManager.getCleanIndicesAtTimestamp(tx.timestamp)
            for (originalGraphStep in TraversalHelper.getStepsOfClass(GraphStep::class.java, traversal)) {
                originalGraphStep as GraphStep<Any, Element>

                val previousStep = originalGraphStep.previousStep
                val isStartStep = previousStep == null || previousStep is EmptyStep<*,*> || previousStep is StartStep<*>
                if(!isStartStep){
                    // intermediate ...step().V().step()... are NOT supported!
                    // if this should ever change, please update the "isStartStep" boolean in the super() constructor of ChronoGraphStep!
                    continue
                }

                val availableIndices = filterIndicesForGraphStep(originalGraphStep, cleanIndices)

                // we do not perform any optimization if the original graph step has source IDs
                // (because we do not need the secondary indices in this case).
                val ids = originalGraphStep.ids
                if (!ids.isNullOrEmpty()) {
                    continue
                }
                val indexableSteps = mutableListOf<FilterStep<Element>>()
                // starting from the initial graph step, check which steps are indexable.
                // We separate the "indexable" check from the "indexed" check, because we can
                // potentially apply a re-ordering of filters here.
                // For example:
                //
                //    g.traversal().V().has("X", 1).has("Y", 2).has("Z", 3).out().has("alpha", 4).toSet()
                //
                // Let's assume that X and Z are indexed, but Y is not. Clearly, we want
                // the index query to be "X == 1 AND Z == 3", and has("Y",2) should be an in-memory filter.
                // To achieve this, we "temporarily" also consider has("Y",2) in order to "step over" it.
                //
                // Also note that the indexable check STOPS at the first step which is non-indexable,
                // therefore in the example above, has("alpha", 4) will never be reached because we stop
                // our search at ".out()".
                var currentStep: Step<*, *> = originalGraphStep.nextStep
                while (ChronoGraphTraversalUtil.isChronoGraphIndexable(currentStep, false)) {
                    if (indexableSteps.isNotEmpty() && indexableSteps[indexableSteps.size - 1].labels.isNotEmpty()) {
                        // the previous step had a label -> stop collecting!
                        break
                    }
                    indexableSteps.add(currentStep as FilterStep<Element>)
                    currentStep = currentStep.nextStep
                }
                if (indexableSteps.isEmpty()) {
                    // the initial steps of the traversal are not indexable, nothing we can do here.
                    // This happens for example in: "g.traversal().V().out()", because "out()" is non-indexable.
                    continue
                }

                // from the list of indexable steps, determine which ones are actually indexed, and remove those from the traversal
                val indexedSteps = indexableSteps.filter { ChronoGraphTraversalUtil.isCoveredByIndices(it, availableIndices) }
                if (indexedSteps.isEmpty()) {
                    // we have steps which would theoretically indexable, but we lack
                    // the necessary (clean) indices, so there's nothing we can do about this.
                    // This happens for example if we have "g.traversal().V().has("type", "A")", but
                    // the property "type" has no index, or the index is dirty.
                    continue
                }

                // remove the steps which are covered by the index from the traversal and collect them in our root step
                indexedSteps.forEach { traversal.removeStep<Any, Any>(it) }
                val chronoGraphStep = ChronoGraphStep(originalGraphStep, indexedSteps)
                TraversalHelper.replaceStep(originalGraphStep, chronoGraphStep, traversal)
            }
        }
    }

    private fun filterIndicesForGraphStep(
        originalGraphStep: GraphStep<*, *>,
        cleanIndices: MutableSet<ChronoGraphIndex>,
    ): Set<ChronoGraphIndex> {
        return when {
            // the graph step is a "V()" -> only use vertex indices
            originalGraphStep.returnsVertex() -> cleanIndices.asSequence()
                .filter { Vertex::class.java.isAssignableFrom(it.indexedElementClass) }
                .toSet()
            // the graph step is a "E()" -> only use edge indices
            originalGraphStep.returnsEdge() -> cleanIndices.asSequence()
                .filter { Edge::class.java.isAssignableFrom(it.indexedElementClass) }
                .toSet()
            else -> emptySet()
        }
    }
}