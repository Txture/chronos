package org.chronos.chronograph.internal.impl.optimizer.strategy

import com.google.common.annotations.VisibleForTesting
import org.apache.tinkerpop.gremlin.process.traversal.*
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Element
import org.chronos.chronodb.internal.impl.query.TextMatchMode
import org.chronos.chronograph.api.builder.query.*
import org.chronos.chronograph.api.exceptions.ChronoGraphException
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.impl.query.ChronoCompare
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare
import org.chronos.chronograph.internal.impl.util.ChronoGraphTraversalUtil
import java.util.function.BiPredicate
import kotlin.math.abs

/**
 * Performs sorting on initial filtering that occurs directly after `traversal.V()` or `traversal().E()`.
 *
 * The desired query structure after applying this strategy is:
 *
 * - `traversal.V()` / `traversal.E()`
 * - `.has(x, P)`    ... where 'x' is an indexed property key. No 'x' occurs twice in this group.
 * - `.has(y, P)`    ... where 'y' is a non-indexed property key. No 'y' occurs twice in this group.
 * - `.filter(...)`
 * - ... rest of the traversal
 *
 * The purpose of this strategy is to act as a preprocessing step to the [ChronoGraphStepStrategy] which
 * will then take those initial `.has(...)` clauses and convert them into index accesses.
 *
 * Most notably:
 * - it is safe to rewrite `.filter(...).has(x = y)` into `.has(x=y).filter(...)`.
 * - there's no point in doing `.has(x=y).has(x=y)`, reduce to a single `.has(x=y)`
 * - `.has(x=y).has(a=b)` is worse than `.has(a=b).has(x=y)` if `a` is indexed and `x` is not, so swap them.
 *
 *
 * **IMPORTANT:** as soon as we encounter any step that has a LABEL attached to it, we must not touch this
 * step and must not look beyond it. Moving around a labelled step will change the outcome of the query.
 *
 */
object OrderFiltersStrategy : AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy?>(),
    TraversalStrategy.ProviderOptimizationStrategy {


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
        val cleanIndices = graph.getIndexManagerOnBranch(tx.branchName).getCleanIndicesAtTimestamp(tx.timestamp)
        try {
            optimize(traversal, cleanIndices.asSequence().map { it.indexedProperty }.toSet())
        } catch (e: Exception) {
            throw ChronoGraphException("Failed to optimize ChronoGraph gremlin query [${traversal}]. Reason: ${e}", e)
        }
    }

    @VisibleForTesting
    fun optimize(traversal: Traversal.Admin<*, *>, indexPropertyKeys: Set<String>) {
        for (graphStep in TraversalHelper.getStepsOfClass(GraphStep::class.java, traversal)) {
            val reorderableSteps = this.findReorderableSteps(graphStep)
            if (reorderableSteps.isEmpty()) {
                // nothing to reorder
                return
            }

            // TODO: Implement DeMorgan to push negations down, convert query to Conjunctive Normal Form
            // push negations as far inside as possible (DeMorgan)
            // this.pushNegationsInside(traversal, reorderableSteps)

            this.unwrapAndP(reorderableSteps)

            // take AndP and convert it into successive filters (e.g. .has(And(x = y, a = b)) => .has(x = y).has(a = b))
            this.unwrapAndStep(traversal, reorderableSteps)

            // technically, a single HasStep can contain multiple HasContainers (i.e. conditions). Split them
            // up such that each HasStep only contains a single HasContainer.
            this.splitHasStepsThatContainMultipleConditions(traversal, graphStep)

            // move '.has' to the front, everything else to the back
            val hasSteps = this.sortHasStepsToTheFront(traversal, indexPropertyKeys, graphStep)

            // eliminate duplicate keys in '.has' (duplicates always occur in direct succession after sorting)
            this.eliminateDuplicateHasSteps(traversal, hasSteps)
        }
    }

    @VisibleForTesting
    fun findReorderableSteps(graphStep: GraphStep<*, *>): MutableList<FilterStep<*>> {
        val reorderableSteps = mutableListOf<FilterStep<*>>()
        var currentStep: Step<*, *>? = graphStep.nextStep
        while (currentStep is FilterStep<*> && canReorder(currentStep)) {
            reorderableSteps += currentStep
            currentStep = currentStep.nextStep
        }
        return reorderableSteps
    }

    private fun canReorder(step: Step<*, *>): Boolean {
        if (step.labels.isNotEmpty()) {
            // no reordering is allowed for steps that have labels.
            return false
        }
        if (step is Mutating<*>) {
            // never step over mutation steps. "drop" is particularly nasty because
            // it IS a FilterStep, but also mutating.
            return false
        }
        if (step is Seedable) {
            // if there's randomness involved, don't step over it.
            // Example: coin() is a FilterStep, but also involves randomness.
            return false
        }
        if (step !is FilterStep) {
            return false
        }
        // do NOT step over lambdas, as they might have side-effects.
        if (step is LambdaFilterStep) {
            return false
        }
        // reordering ".and(...)" and ".or(...)" is only allowed if all
        // individual steps used in them are reorderable.
        if (step is ConnectiveStep<*>) {
            return step.localChildren.all { subTraversal -> subTraversal.steps.all { canReorder(it) } }
        }

        return true
    }

    @VisibleForTesting
    fun unwrapAndP(reorderableSteps: List<FilterStep<*>>) {
        for (step in reorderableSteps.toList()) {
            if (step is HasStep<*>) {
                val andPContainers = step.hasContainers.filter { it.predicate is AndP }
                for (andPContainer in andPContainers) {
                    val andP = andPContainer.predicate as AndP
                    for (predicate in flattenAndP(andP)) {
                        step.addHasContainer(HasContainer(andPContainer.key, predicate))
                    }
                }
                andPContainers.forEach(step::removeHasContainer)
            } else if (step is ConnectiveStep<*>) {
                for (childTraversal in step.localChildren) {
                    // in the child traversal, all steps are filters.
                    unwrapAndP(childTraversal.steps as List<FilterStep<*>>)
                }
            }
        }
    }

    @VisibleForTesting
    fun flattenAndP(andP: AndP<*>): List<P<*>> {
        return andP.predicates.asSequence().flatMap {
            if (it is AndP) {
                flattenAndP(it).asSequence()
            } else {
                sequenceOf(it)
            }
        }.toList()
    }

    @Suppress("UNCHECKED_CAST")
    private fun unwrapAndStep(traversal: Traversal.Admin<*, *>, reorderableSteps: MutableList<FilterStep<*>>) {
        var changed = true
        while (changed) {
            changed = false
            for (step in reorderableSteps.toList()) {
                if (step !is AndStep) {
                    continue
                }
                val stepIndexInList = reorderableSteps.indexOf(step)
                for (childTraversal in step.localChildren) {
                    for (childStep: Step<Any, Any> in childTraversal.steps.asReversed()) {
                        TraversalHelper.insertAfterStep(childStep, step as Step<Any, Any>, traversal)
                        reorderableSteps.add(stepIndexInList + 1, childStep as FilterStep<Any>)
                    }
                }
                traversal.removeStep<Any, Any>(step)
                reorderableSteps.remove(step)
                changed = true
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun splitHasStepsThatContainMultipleConditions(
        traversal: Traversal.Admin<*, *>,
        graphStep: GraphStep<*, *>,
    ) {
        val reorderableSteps = this.findReorderableSteps(graphStep)
        for (step in reorderableSteps.toList()) {
            if (step is HasStep<*> && step.hasContainers.size > 1) {
                // create one step per HasContainer.
                for (hasContainer in step.hasContainers) {
                    val newStep = HasStep<Element>(traversal, hasContainer)
                    TraversalHelper.insertBeforeStep(newStep as Step<Any, Any>, step as Step<Any, *>, traversal)
                }
                // delete the original step in the traversal
                traversal.removeStep<Any, Any>(step)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun sortHasStepsToTheFront(
        traversal: Traversal.Admin<*, *>,
        indexedPropertyKeys: Set<String>,
        graphStep: GraphStep<*, *>,
    ): List<HasStep<*>> {
        val reorderableSteps = this.findReorderableSteps(graphStep)
        val hasSteps = reorderableSteps.asSequence()
            .filterIsInstance<HasStep<*>>()
            .sortedWith(HasStepComparator(indexedPropertyKeys))
            .toList()

        // we reverse the list here because the last entry that is being processed
        // will end up as the first operation after .V() / .E().
        for (hasStep in hasSteps.asReversed()) {
            traversal.removeStep<Any, Any>(hasStep)
            TraversalHelper.insertAfterStep(hasStep as Step<Any, Any>, graphStep as Step<Any, Any>, traversal)
        }
        return hasSteps
    }

    @VisibleForTesting
    fun eliminateDuplicateHasSteps(traversal: Traversal.Admin<*, *>, hasSteps: List<HasStep<*>>) {
        // since we sorted the steps beforehand, duplicates will now occur one after the other,
        // for example: a-b-b-b-c-c-d-e-e-f-g
        var index = 0
        while (index < hasSteps.size) {
            val currentHas = hasSteps[index]
            var nextIndex = index + 1
            while (nextIndex < hasSteps.size) {
                val nextHas = hasSteps[nextIndex]
                if (this.areHasStepsEqual(currentHas, nextHas)) {
                    // make sure we skip this step in the main loop iteration
                    index++
                    // peek the next step
                    nextIndex++
                    // remove this step from the traversal, it's a duplicate of "currentHas"
                    traversal.removeStep<Any, Any>(nextHas)
                } else {
                    // this one's different, don't merge!
                    break
                }
            }
            index++
        }
    }

    private fun areHasStepsEqual(left: HasStep<*>, right: HasStep<*>): Boolean {
        val leftContainers = left.hasContainers
        val rightContainers = right.hasContainers
        if (leftContainers.size != rightContainers.size) {
            return false
        }
        for (index in leftContainers.indices) {
            val leftContainer = leftContainers[index]
            val rightContainer = rightContainers[index]
            if (!this.areHasContainersEqual(leftContainer, rightContainer)) {
                return false
            }
        }
        return true
    }

    private fun areHasContainersEqual(left: HasContainer, right: HasContainer): Boolean {
        if (left.key != right.key) {
            return false
        }
        if (left.predicate != right.predicate) {
            return false
        }
        return true
    }

    @VisibleForTesting
    class HasStepComparator(
        private val indexedKeys: Set<String>,
    ) : Comparator<HasStep<*>> {

        override fun compare(left: HasStep<*>, right: HasStep<*>): Int {
            // we ensured previously that each HasStep only contains one HasContainer.
            val leftHas = left.hasContainers.single()
            val rightHas = right.hasContainers.single()
            if (leftHas.key == rightHas.key) {
                // the two has containers have the same key. E.g. has(x > 3) and has(x < 14). Order them by condition.
                return this.compareConditions(leftHas.predicate, rightHas.predicate)
            }
            // the keys are different. Check if they're indexed
            val leftIsIndexed = indexedKeys.contains(leftHas.key)
            val rightIsIndexed = indexedKeys.contains(rightHas.key)
            if (leftIsIndexed == rightIsIndexed) {
                // if both (or neither) are indexed, compare alphabetically
                return leftHas.key.compareTo(rightHas.key)
            }
            return if (leftIsIndexed) {
                // the left key is indexed, the right key is not -> order the left one to the front
                -1
            } else {
                // the left key is not indexed, the right key is -> order the right one to the front
                +1
            }
        }

        private fun compareConditions(left: P<*>, right: P<*>): Int {
            // here we don't really care which operation comes first,
            // the most important part is that the same operations occur
            // right after one another in the ordered list (because this allows
            // for easy detection of duplicates).
            if (left is ConnectiveP && right is ConnectiveP) {
                // both are AND/OR conditions
                return when {
                    left is AndP && right is AndP -> {
                        // compare the individual components. Prefer "simple" ones (fewer predicates)
                        compareConnectiveP(left, right)
                    }

                    left is OrP && right is OrP -> {
                        // compare individual components
                        compareConnectiveP(left, right)
                    }

                    else -> {
                        // we're comparing AND with OR, sort ANDs to the front
                        if (left is AndP) {
                            -1
                        } else {
                            +1
                        }
                    }
                }
            } else if (left is ConnectiveP) {
                // left is AND/OR, right is not. Sort the right one to the front
                return +1
            } else if (right is ConnectiveP) {
                // right is AND/OR, left is not. Sort the left one to the front
                return -1
            }

            val leftPredicateOrderIndex = getOrderIndex(left)
            val rightPredicateOrderIndex = getOrderIndex(right)
            if (leftPredicateOrderIndex < rightPredicateOrderIndex) {
                return -1
            } else if (leftPredicateOrderIndex > rightPredicateOrderIndex) {
                return +1
            }
            // the predicates are the same, check the values
            val leftValue = left.value
            val rightValue = right.value
            // we don't really care about the order, the only condition is that
            // equal values occur one after another.
            return leftValue.hashCode().compareTo(rightValue.hashCode())
        }

        private fun compareConnectiveP(left: ConnectiveP<*>, right: ConnectiveP<*>): Int {
            val leftPredicates = left.predicates
            val rightPredicates = right.predicates
            if (leftPredicates.size < rightPredicates.size) {
                return -1
            } else if (leftPredicates.size > rightPredicates.size) {
                return +1
            }
            // both have the same number of predicates
            for (index in leftPredicates.indices) {
                val leftChildP = leftPredicates[index]
                val rightChildP = rightPredicates[index]
                val comparison = compareConditions(leftChildP, rightChildP)
                if (comparison != 0) {
                    return comparison
                }
            }
            return 0
        }

        private fun getOrderIndex(p: P<*>): Int {
            if (p is AndP) {
                return p.predicates.fold(1) { product, localP -> product + 31 * getOrderIndex(localP) }
            }
            if (p is OrP) {
                return p.predicates.fold(1) { product, localP -> product + 17 * getOrderIndex(localP) }
            }
            return getOrderIndex(p.biPredicate)
        }

        private fun getOrderIndex(predicate: BiPredicate<*, *>): Int {
            return when (predicate) {
                Compare.eq, ChronoCompare.EQ -> 1
                Compare.neq, ChronoCompare.NEQ -> 2
                Compare.gt, ChronoCompare.GT -> 3
                Compare.gte, ChronoCompare.GTE -> 4
                Compare.lt, ChronoCompare.LT -> 5
                Compare.lte, ChronoCompare.LTE -> 6
                Text.startingWith, ChronoStringCompare.STRING_STARTS_WITH -> 7
                ChronoStringCompare.STRING_STARTS_WITH_IGNORE_CASE -> 8

                Text.endingWith, ChronoStringCompare.STRING_ENDS_WITH -> 9
                ChronoStringCompare.STRING_ENDS_WITH_IGNORE_CASE -> 10

                Text.containing, ChronoStringCompare.STRING_CONTAINS -> 11
                ChronoStringCompare.STRING_CONTAINS_IGNORE_CASE -> 12

                Text.notContaining, ChronoStringCompare.STRING_NOT_CONTAINS -> 13
                ChronoStringCompare.STRING_NOT_CONTAINS_IGNORE_CASE -> 14

                Text.notEndingWith, ChronoStringCompare.STRING_NOT_ENDS_WITH -> 15
                ChronoStringCompare.STRING_NOT_ENDS_WITH_IGNORE_CASE -> 16

                Text.notStartingWith, ChronoStringCompare.STRING_NOT_STARTS_WITH -> 17
                ChronoStringCompare.STRING_NOT_STARTS_WITH_IGNORE_CASE -> 18

                ChronoStringCompare.STRING_MATCHES_REGEX -> 19
                ChronoStringCompare.STRING_MATCHES_REGEX_IGNORE_CASE -> 20

                ChronoStringCompare.STRING_NOT_MATCHES_REGEX -> 21
                ChronoStringCompare.STRING_NOT_MATCHES_REGEX_IGNORE_CASE -> 22

                ChronoCompare.WITHIN -> 23
                ChronoCompare.WITHOUT -> 24

                is StringWithinCP -> when (predicate.matchMode) {
                    null, TextMatchMode.STRICT -> 25
                    TextMatchMode.CASE_INSENSITIVE -> 26
                }

                is StringWithoutCP -> when (predicate.matchMode) {
                    null, TextMatchMode.STRICT -> 27
                    TextMatchMode.CASE_INSENSITIVE -> 28
                }

                is DoubleWithinCP -> 29
                is DoubleWithoutCP -> 30
                is DoubleEqualsCP -> 31
                is DoubleNotEqualsCP -> 32

                // some other predicate we don't know...? Ensure we have no
                // collisions with the known ones.
                else -> 100 + abs(predicate.hashCode())
            }
        }


    }
}