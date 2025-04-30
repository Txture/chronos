package org.chronos.chronograph.internal.impl.optimizer.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Step
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphPrefetchingBarrierStep
import org.chronos.chronograph.internal.impl.optimizer.step.Prefetching

object TraversalStrategyUtils {

    /** The maximum number of elements to hold in a prefetching barrier.*/
    const val PREFETCH_BARRIER_MAX_SIZE = Int.MAX_VALUE

    val Traversal.Admin<*, *>.options: Map<String, Any>?
        get() = this.strategies.getStrategy(OptionsStrategy::class.java).orElse(null)?.options

    fun <T, S, E> insertPrefetchingBarrierBefore(prefetchingStep: T)
        where T : Prefetching, T : Step<S, E> {
        val traversal = prefetchingStep.getTraversal<S, E>()
        val prefetchingBarrier = ChronoGraphPrefetchingBarrierStep<S>(
            traversal = traversal,
            maxBarrierSize = PREFETCH_BARRIER_MAX_SIZE,
            prefetchingStep
        )
        TraversalHelper.insertBeforeStep(prefetchingBarrier, prefetchingStep, traversal)
    }


    fun getDownstreamStepsOf(step: Step<*, *>): List<Step<*, *>> {
        val sameTraversalDownstream = this.getDownstreamStepsInSameTraversalOf(step)
        val lastStep = sameTraversalDownstream.lastOrNull() ?: step
        if (lastStep is EmptyStep<*, *>) {
            return sameTraversalDownstream
        }
        val parentStep = (lastStep.getTraversal<Any, Any>()?.parent as? Step<*, *>)?.emptyStepToNull()
        return if (parentStep != null) {
            // take our result and add to it the result of the parent step
            sameTraversalDownstream + getDownstreamStepsOf(parentStep)
        } else {
            sameTraversalDownstream
        }
    }


    fun getDownstreamStepsInSameTraversalOf(step: Step<*, *>): List<Step<*, *>> {
        if (step is EmptyStep) {
            return emptyList()
        }
        var current: Step<*, *>? = step.nextStep.emptyStepToNull()
            ?: return emptyList()
        val resultList = mutableListOf<Step<*, *>>()
        while (current != null) {
            resultList += current
            current = current.nextStep.emptyStepToNull()
        }
        return resultList
    }

    private fun <S, E> Step<S, E>?.emptyStepToNull(): Step<S, E>? {
        return if (this is EmptyStep<S, E>) {
            null
        } else {
            this
        }
    }
}