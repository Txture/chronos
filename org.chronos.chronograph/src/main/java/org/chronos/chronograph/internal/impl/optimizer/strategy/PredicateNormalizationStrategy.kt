package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil

object PredicateNormalizationStrategy: ChronoGraphStrategy() {

    override fun apply(traversal: Traversal.Admin<*, *>?) {
        for (originalGraphStep in TraversalHelper.getStepsOfClass(GraphStep::class.java, traversal)) {
            var currentStep: Step<*, *> = originalGraphStep.nextStep
            val indexableSteps = mutableListOf<Step<*, *>>()
            // note that we MUST NOT perform any optimization here if we have a LABEL present on the step!
            while (ChronoGraphTraversalUtil.isChronoGraphIndexable(currentStep, true) && currentStep.labels.isEmpty()) {
                indexableSteps.add(currentStep)
                currentStep = currentStep.nextStep
            }
            val replaceSteps = ChronoGraphTraversalUtil.normalizeConnectivePredicates(traversal, indexableSteps)
            for ((key, value) in replaceSteps) {
                TraversalHelper.replaceStep(key, value, traversal)
            }
        }
    }
}