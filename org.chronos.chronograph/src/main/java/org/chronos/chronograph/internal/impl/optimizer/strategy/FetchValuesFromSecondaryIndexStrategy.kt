package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphOrderGlobalStep
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphPropertiesStep
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphPropertyMapStep
import java.util.*

@Suppress("UNCHECKED_CAST")
object FetchValuesFromSecondaryIndexStrategy : ChronoGraphStrategy() {

    override fun apply(traversal: Traversal.Admin<*, *>) {
        val options = traversal.strategies.getStrategy(OptionsStrategy::class.java).orElse(null)?.options
        val graph = traversal.chronoGraph

        val useSecondaryIndexForValuesStep = this.shouldUseSecondaryIndexForValuesStep(options, graph)
        val useSecondaryIndexForValueMapStep = this.shouldUseSecondaryIndexForValueMapStep(options, graph)

        if (useSecondaryIndexForValuesStep) {
            this.replacePropertiesSteps(traversal)
        }
        if (useSecondaryIndexForValueMapStep) {
            this.replacePropertyMapSteps(traversal)
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
        val traversalSetting = options?.get(ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP) as? Boolean?
        // try to find the (global) setting in the graph
        val globalGraphSetting = graph?.chronoGraphConfiguration?.isUseSecondaryIndexForGremlinValueMapStep
        return traversalSetting
            ?: globalGraphSetting
            // if all else fails, use the gremlin standard behaviour.
            ?: false
    }

    private fun replacePropertiesSteps(traversal: Traversal.Admin<*, *>) {
        for (propertyStep in TraversalHelper.getStepsOfClass(PropertiesStep::class.java, traversal)) {
            // Replace the Gremlin ".values" step (which is officially called "PropertiesStep") with
            // our implementation that can potentially use the secondary index.
            TraversalHelper.replaceStep(
                propertyStep as PropertiesStep<Any>,
                ChronoGraphPropertiesStep(
                    traversal,
                    propertyStep.returnType,
                    propertyStep.propertyKeys,
                    propertyStep.labels
                ),
                traversal
            )
        }
    }

    private fun replacePropertyMapSteps(traversal: Traversal.Admin<*, *>) {
        for (propertyMapStep in TraversalHelper.getStepsOfClass(PropertyMapStep::class.java, traversal)) {
            // Replace the Gremlin ".valueMap" step (which is officially called "PropertyMapStep") with
            // our implementation that can potentially use the secondary index.
            TraversalHelper.replaceStep(
                propertyMapStep,
                ChronoGraphPropertyMapStep(
                    traversal,
                    propertyMapStep.returnType,
                    propertyMapStep.includedTokens,
                    propertyMapStep.propertyKeys,
                    propertyMapStep.labels
                ),
                traversal
            )
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

}