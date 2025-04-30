package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderLocalStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Element
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration
import org.chronos.chronograph.internal.impl.optimizer.step.*
import org.chronos.chronograph.internal.impl.optimizer.strategy.TraversalStrategyUtils.PREFETCH_BARRIER_MAX_SIZE
import org.chronos.chronograph.internal.impl.optimizer.strategy.TraversalStrategyUtils.options
import java.util.*

@Suppress("UNCHECKED_CAST")
object FetchValuesFromSecondaryIndexStrategy : ChronoGraphStrategy() {

    override fun applyPrior(): Set<Class<out TraversalStrategy.ProviderOptimizationStrategy>> {
        return setOf(
            ChronoGraphStepStrategy::class.java,
            PredicateNormalizationStrategy::class.java,
            ReplaceGremlinPredicateWithChronosPredicateStrategy::class.java,
        )
    }

    override fun apply(traversal: Traversal.Admin<*, *>) {
        if (traversal !is GraphTraversal<*, *>) {
            // don't apply this strategy to value traversals.
            return
        }

        val options = traversal.options
        val graph = traversal.chronoGraph

        val useSecondaryIndexForValuesStep = this.shouldUseSecondaryIndexForValuesStep(options, graph)
        val useSecondaryIndexForValueMapStep = this.shouldUseSecondaryIndexForValueMapStep(options, graph)
        val usePrefetching = useSecondaryIndexForValueMapStep || useSecondaryIndexForValuesStep

        if (useSecondaryIndexForValuesStep) {
            this.replacePropertiesSteps(traversal)
        }
        if (useSecondaryIndexForValueMapStep) {
            this.replacePropertyMapSteps(traversal)
        }

        if (usePrefetching) {
            this.applyPrefetchingForSubTraversals(traversal)
        }

        this.replaceOrderGlobalSteps(traversal)
    }

    private fun shouldUseSecondaryIndexForValuesStep(options: Map<String, Any>?, graph: ChronoGraph?): Boolean {
        // try to find the setting locally in our traversal
        val traversalSetting = options?.get(ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP) as? Boolean?
        // try to find the (global) setting in the graph
        val globalGraphSetting = graph?.chronoGraphConfiguration?.isUseSecondaryIndexForGremlinValuesStep
        return traversalSetting
            ?: globalGraphSetting
            // if all else fails, use the gremlin standard behaviour.
            ?: false
    }

    private fun shouldUseSecondaryIndexForValueMapStep(options: Map<String, Any>?, graph: ChronoGraph?): Boolean {
        // try to find the setting locally in our traversal
        val traversalSetting =
            options?.get(ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP) as? Boolean?
        // try to find the (global) setting in the graph
        val globalGraphSetting = graph?.chronoGraphConfiguration?.isUseSecondaryIndexForGremlinValueMapStep
        return traversalSetting
            ?: globalGraphSetting
            // if all else fails, use the gremlin standard behaviour.
            ?: false
    }

    private fun replacePropertiesSteps(traversal: Traversal.Admin<*, *>) {
        for (propertyStep in TraversalHelper.getStepsOfAssignableClassRecursively(
            PropertiesStep::class.java,
            traversal
        )) {
            if (propertyStep is ChronoGraphPropertiesStep<*>) {
                // this step has already been transformed
                continue
            }
            // Replace the Gremlin ".values()" step (which is internally called "PropertiesStep") with
            // our implementation that can potentially use the secondary index.
            val targetTraversal = propertyStep.getTraversal<Any, Any>()
            val chronoGraphPropertiesStep = ChronoGraphPropertiesStep<Any>(
                traversal = targetTraversal,
                propertyType = propertyStep.returnType,
                propertyKeys = propertyStep.propertyKeys,
                labels = propertyStep.labels
            )
            TraversalHelper.replaceStep(
                propertyStep as PropertiesStep<Any>,
                chronoGraphPropertiesStep,
                targetTraversal
            )
            // this enables the step to be Prefetching. Now, we need to "feed" it the
            // proper graph elements to prefetch. We do this by injecting a barrier step
            // which waits for incoming elements, registering them for prefetching. Only
            // when the barrier is released (i.e. all input elements have arrived), we
            // allow them to continue to the ChronoGraphPropertiesStep, which is now
            // pre-initialized with the prefetching information.
            TraversalStrategyUtils.insertPrefetchingBarrierBefore(chronoGraphPropertiesStep)
        }
    }


    private fun replacePropertyMapSteps(traversal: Traversal.Admin<*, *>) {
        for (propertyMapStep in TraversalHelper.getStepsOfAssignableClassRecursively(
            PropertyMapStep::class.java,
            traversal
        )) {
            // find the traversal of the step we're about to replace. This may be the main traversal or a sub-traversal.
            val targetTraversal = propertyMapStep.getTraversal<Any, Any>()
            // Replace the Gremlin ".valueMap" step (which is officially called "PropertyMapStep") with
            // our implementation that can potentially use the secondary index.
            val chronoGraphPropertyMapStep = ChronoGraphPropertyMapStep<Any, Any>(
                targetTraversal,
                propertyMapStep.returnType,
                propertyMapStep.includedTokens,
                propertyMapStep.propertyKeys,
                propertyMapStep.labels
            )
            TraversalHelper.replaceStep(
                propertyMapStep as PropertyMapStep<Any, Any>,
                chronoGraphPropertyMapStep,
                targetTraversal
            )
            // this enables the step to be Prefetching. Now, we need to "feed" it the
            // proper graph elements to prefetch. We do this by injecting a barrier step
            // which waits for incoming elements, registering them for prefetching. Only
            // when the barrier is released (i.e. all input elements have arrived), we
            // allow them to continue to the ChronoGraphPropertiesStep, which is now
            // pre-initialized with the prefetching information.
            TraversalStrategyUtils.insertPrefetchingBarrierBefore(chronoGraphPropertyMapStep)
        }
    }

    private fun replaceOrderGlobalSteps(traversal: Traversal.Admin<*, *>) {
        for (orderGlobalStep in TraversalHelper.getStepsOfClass(OrderGlobalStep::class.java, traversal)) {
            orderGlobalStep as OrderGlobalStep<Any, Comparable<*>>
            // the "random" field is unfortunately private and has no getter...
            val random = orderGlobalStep.javaClass
                .getDeclaredField("random")
                .also { it.isAccessible = true }
                .get(orderGlobalStep) as Random
            TraversalHelper.replaceStep(
                orderGlobalStep,
                ChronoGraphOrderGlobalStep(
                    traversal = traversal,
                    comparators = orderGlobalStep.comparators,
                    limit = orderGlobalStep.limit,
                    random = random,
                    labels = orderGlobalStep.labels
                ),
                traversal
            )
        }
    }

    private fun applyPrefetchingForSubTraversals(traversal: Traversal.Admin<*, *>) {
        // The problem with sub-traversals is that gremlin "triggers" the
        // sub-traversal for each traverser individually. For example:
        //
        // graph.traversal()
        //      .V()
        //      .has("firstName", "John")
        //      .sideEffect(
        //          valueMap("lastName", "birthday").aggregate("mySideEffect")
        //      ).cap("mySideEffect")
        //      .next()
        //
        // In this query, the "valueMap(...)" step will see the vertices
        // from the main traversal one by one. Even if we introduce a
        // barrier step right at the start of the sub-traversal, the
        // barrier will only ever have size 1 before getting released.
        //
        // This situation is bad for performance, as it forces the
        // "valueMap()" step in the sub-traversal to resolve the
        // vertices one by one, even though there may be a secondary
        // index on "lastName" and "birthday".
        //
        // To resolve the issue, we introduce a barrier step BEFORE
        // the side effect, and we register all traversers in the full
        // barrier for prefetching in the sub-traversal.

        for (traversalParent in TraversalHelper.getStepsOfAssignableClass(TraversalParent::class.java, traversal)) {
            val prefetchingStepsInChildTraversals = mutableListOf<Prefetching>()
            val allChildren = mutableSetOf<Traversal.Admin<*, *>>()
            allChildren.addAll(traversalParent.getGlobalChildren<Any, Any>())
            allChildren.addAll(traversalParent.getLocalChildren<Any, Any>())
            for (childTraversal in allChildren) {
                // first, do the same process for the children.
                applyPrefetchingForSubTraversals(childTraversal)
                // determine which child step (if any) requires prefetched data
                val prefetchingStep = findFirstPrefetchingStepInTraversal(childTraversal)
                    ?: continue // child traversal doesn't require (or support) prefetching
                prefetchingStepsInChildTraversals.add(prefetchingStep)
            }
            if (prefetchingStepsInChildTraversals.isEmpty()) {
                // none of our child traversals require (or support) prefetching.
                continue
            }

            // we've identified a prefetching step in at least one of the child traversals.
            // This means that we have to insert a prefetching barrier step in the parent
            // traversal before this child is called, and allow the child to prefetch.
            val stepWithPrefetchingChildren = traversalParent.asStep() as Step<Element, Element>

            val previousStep = stepWithPrefetchingChildren.previousStep
            if (previousStep is ChronoGraphPrefetchingBarrierStep<*>) {
                // the step which precedes us is already a prefetching barrier
                previousStep.addClients(prefetchingStepsInChildTraversals)
            } else {
                val prefetchingBarrier = ChronoGraphPrefetchingBarrierStep<Element>(
                    traversal = traversal,
                    maxBarrierSize = PREFETCH_BARRIER_MAX_SIZE,
                    clients = prefetchingStepsInChildTraversals, // the steps in the child traversals are our clients
                )

                TraversalHelper.insertBeforeStep(prefetchingBarrier, stepWithPrefetchingChildren, traversal)
            }
        }
    }

    private fun findFirstPrefetchingStepInTraversal(traversal: Traversal.Admin<*, *>): Prefetching? {
        for (step in traversal.steps) {
            if (step is Prefetching) {
                // we've arrived at a prefetching step.
                return step
            } else if (!this.canPrefetchingFlowThrough(step)) {
                // the traversal may need prefetching, but we've hit a step which doesn't
                // allow us to determine WHAT needs to be prefetched. For example, in:
                //
                //  out().valueMap("x", "y")
                //
                // the "out()" prevents prefetching in the "valueMap(...)", because it modifies
                // the traverser set in unpredictable ways. We can't do anything with prefetching
                // here, don't continue.
                return null
            }
        }
        // this traversal doesn't need prefetching at all.
        return null
    }

    private fun canPrefetchingFlowThrough(step: Step<*, *>): Boolean {
        return when (step) {
            // for filter steps, we may end up pre-fetching *too much*, but we can still do it.
            is FilterStep<*> -> true
            // barriers don't change the result set, prefetching is still an option.
            is NoOpBarrierStep<*> -> true
            // identity steps are no-ops, so prefetching is still an option.
            is IdentityStep<*> -> true
            // sorting steps don't change the result set, prefetching is still an option.
            is OrderLocalStep<*, *> -> true
            is OrderGlobalStep<*, *> -> true
            // performing a side-effect doesn't change the result set, prefetching is still an option.
            is SideEffectStep<*> -> true
            // profiling doesn't change the result set
            is ProfileStep<*> -> true
            // attaching labels doesn't change the result set
            is LabelStep<*> -> true
            // all other steps modify the result set in some way, so prefetching after them is generally infeasible.
            else -> false
        }
    }


}