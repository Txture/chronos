package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Element
import org.chronos.chronograph.api.index.ChronoGraphIndex
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphIndexedHasStep
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphPrefetchingBarrierStep
import org.chronos.chronograph.internal.impl.optimizer.strategy.TraversalStrategyUtils.PREFETCH_BARRIER_MAX_SIZE
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.concurrent.Callable

/**
 * This strategy attempts to solve the following problem:
 *
 * - We have many graph elements in the pipeline coming from secondary indices
 *   (i.e. the elements are *not* loaded into memory yet, we only know their IDs)
 *
 * - At some point in the query there's a [HasStep] which cannot be merged into the
 *   basic graph step (e.g. because it is preceded by a `union(...)` step).
 *
 * - The [HasStep] will resolve each element individually and then apply its filter condition,
 *   ignoring the fact that a secondary index might exist
 *
 * - This results in poor query performance
 *
 *
 * The [ChronoGraphIndexedHasStep] solves this problem if we replace the original
 * [HasStep] with the indexed version. However, there are restrictions:
 *
 * - Since the [ChronoGraphIndexedHasStep] requires a secondary index in order
 *   to provide meaningful benefits over the regular [HasStep], **all** properties
 *   used by the [HasStep] need to have a secondary index.
 *
 * - Since the [ChronoGraphIndexedHasStep] requires a preceding barrier step, its
 *   output will **not** be lazy anymore. It therefore doesn't make sense to use
 *   it if there's a downstream `.limit(...)` step, because the limit means that
 *   not all elements of the stream will need to be evaluated in the first place.
 *
 * - If the majority of elements which enter the [HasStep] have already been
 *   loaded from disk, it is faster to query their properties directly. This
 *   cannot be decided upfront, but the [ChronoGraphIndexedHasStep] has internal
 *   logic to handle this case.
 *
 */
object UseSecondaryIndexForHasStepsStrategy : ChronoGraphStrategy() {

    override fun applyPrior(): Set<Class<out TraversalStrategy.ProviderOptimizationStrategy?>?>? {
        return setOf(
            ChronoGraphStepStrategy::class.java,
            ReplaceGremlinPredicateWithChronosPredicateStrategy::class.java,
            OrderFiltersStrategy::class.java,
            PredicateNormalizationStrategy::class.java,
            FetchValuesFromSecondaryIndexStrategy::class.java,
        )
    }

    @Suppress("UNCHECKED_CAST")
    override fun apply(traversal: Traversal.Admin<*, *>?) {
        if (traversal !is GraphTraversal<*, *>) {
            return
        }

        val hasSteps = TraversalHelper.getStepsOfClass(
            HasStep::class.java,
            traversal
        )

        if (hasSteps.isEmpty()) {
            return
        }

        val graph = traversal.chronoGraph
            ?: return

        // ensure that we have an open transaction...
        graph.tx().readWrite()

        val transaction = graph.tx().currentTransaction
        val indexManager = graph.getIndexManagerOnBranch(transaction.branchName) as ChronoGraphIndexManagerInternal

        val indices = indexManager.withIndexReadLock(Callable {
            indexManager.getCleanIndicesAtTimestamp(transaction.timestamp)
        })

        for (hasStep in hasSteps) {
            if (!shouldReplaceHasStepWithIndexedHasStep(hasStep, indices)) {
                continue
            }

            // detect the pattern "not(has(...))" because the PrefetchingBarrierStep
            // needs to be inserted before the "not(...)" in order to have the desired effect.
            if (isNestedInNotStep(hasStep)) {
                this.replaceHasStepWithIndexedVersionWithinNot(traversal, hasStep)
            } else {
                this.replaceHasStepWithIndexedVersionFlat(traversal, hasStep)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun replaceHasStepWithIndexedVersionFlat(traversal: Traversal.Admin<*, *>, hasStep: HasStep<*>) {
        val indexedHasStep = ChronoGraphIndexedHasStep<Element>(
            traversal = traversal,
            hasContainers = hasStep.hasContainers.toTypedArray()
        )

        // transfer over the labels
        TraversalHelper.copyLabels(hasStep, indexedHasStep, false)

        TraversalHelper.replaceStep(
            hasStep as Step<Any, Any>,
            indexedHasStep as Step<Any, Any>,
            traversal,
        )

        TraversalStrategyUtils.insertPrefetchingBarrierBefore(indexedHasStep)
    }

    @Suppress("UNCHECKED_CAST")
    private fun replaceHasStepWithIndexedVersionWithinNot(traversal: Traversal.Admin<*, *>, hasStep: HasStep<*>) {
        val parentNotStep = hasStep.getTraversal<Any, Any>().parent as NotStep<Any>
        val indexedHasStep = ChronoGraphIndexedHasStep<Element>(
            traversal = traversal,
            hasContainers = hasStep.hasContainers.toTypedArray()
        )

        // transfer over the labels
        TraversalHelper.copyLabels(hasStep, indexedHasStep, false)

        TraversalHelper.replaceStep(
            hasStep as Step<Any, Any>,
            indexedHasStep as Step<Any, Any>,
            traversal,
        )

        // in front of the parent NOT step, put the prefetching barrier
        val prefetchingBarrier = ChronoGraphPrefetchingBarrierStep<Any>(
            traversal = traversal,
            maxBarrierSize = PREFETCH_BARRIER_MAX_SIZE,
            indexedHasStep
        )
        TraversalHelper.insertBeforeStep(prefetchingBarrier, parentNotStep, parentNotStep.getTraversal<Any, Any>())
    }

    private fun shouldReplaceHasStepWithIndexedHasStep(
        hasStep: HasStep<*>,
        allIndices: Set<ChronoGraphIndex>,
    ): Boolean {
        if (hasStep is ChronoGraphIndexedHasStep<*>) {
            // this one is already using indices, skip.
            return false
        }

        if (!ChronoGraphTraversalUtil.isCoveredByIndices(hasStep, allIndices)) {
            // at least one of the properties is not indexed, which means
            // that it would force us to load the entire graph element anyway.
            return false
        }

        val allPredicatesIndexSupported = hasStep.hasContainers.asSequence()
            .map { it.predicate }
            .all { ChronoGraphTraversalUtil.isChronoGraphIndexablePredicate(it) }

        if (!allPredicatesIndexSupported) {
            return false
        }

        val downstreamSteps = TraversalStrategyUtils.getDownstreamStepsOf(hasStep)

        val isLimitZeroQuery = downstreamSteps.asSequence()
            .takeWhile { it !is InjectStep<*> }
            .filterIsInstance<RangeGlobalStep<*>>()
            .filter { it.highRange <= 0 }
            .any()

        if (isLimitZeroQuery) {
            // the entire query has a "limit(0)" somewhere, so no need
            // to evaluate the HasStep at all (and definitely not eagerly!).
            return false
        }

        val isSmallLimitQuery = downstreamSteps.asSequence()
            // do not "skip over" filters and flatmaps when looking for
            // the limit(...) step, because even if the limit is low, it
            // can mean that a lot of elements would go through the hasStep.
            .takeWhile { it !is FilterStep<*> && it !is FlatMapStep<*, *> && it !is InjectStep<*> }
            .filterIsInstance<RangeGlobalStep<*>>()
            .filter { it.highRange < 1000 }
            .any()

        if (isSmallLimitQuery) {
            // this hasStep is limited in scope, therefore it's
            // not worth it to replace it with a secondary index
            // access.
            return false
        }

        return true
    }

    private fun isNestedInNotStep(hasStep: HasStep<*>): Boolean {
        val parent = hasStep.getTraversal<Any, Any>().parent
        if (parent !is NotStep<*>) {
            // this "has(...)" step is not nested within a "not(...)" step.
            return false
        }

        val previousStep = hasStep.previousStep
        if (previousStep != null && previousStep::class != StartStep::class && previousStep !is EmptyStep<*, *>) {
            // we have something in front of the "has(...)", not good. Be defensive and don't do anything here.
            return false
        }

        // we now know that the pattern "not(has(...), ...)" exists here.
        return true
    }

}