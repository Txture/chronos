package org.chronos.chronograph.internal.impl.optimizer.strategy

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import org.apache.tinkerpop.gremlin.process.traversal.*
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.chronos.chronograph.api.builder.query.*
import org.chronos.chronograph.internal.impl.query.ChronoCompare
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare

object ReplaceGremlinPredicateWithChronosPredicateStrategy : ChronoGraphStrategy() {

    override fun apply(traversal: Traversal.Admin<*, *>?) {
        val hasContainerHolders = TraversalHelper.getStepsOfAssignableClassRecursively(HasContainerHolder::class.java, traversal)
        for (hasContainerHolder in hasContainerHolders) {
            for (container in Lists.newArrayList(hasContainerHolder.hasContainers)) {
                val replacement = replacePredicateInContainer(container)
                // this strict reference equality check is on purpose
                if (container !== replacement) {
                    hasContainerHolder.removeHasContainer(container)
                    hasContainerHolder.addHasContainer(replacement)
                }
            }
        }
    }

    override fun applyPrior(): Set<Class<out ProviderOptimizationStrategy>> {
        val resultSet: MutableSet<Class<out ProviderOptimizationStrategy>> = Sets.newHashSet()
        resultSet.add(PredicateNormalizationStrategy::class.java)
        return resultSet
    }

    private fun replacePredicateInContainer(container: HasContainer): HasContainer {
        val originalPredicate = container.predicate
        val newPredicate = convertPredicate(originalPredicate)
        // this strict reference equality check is on purpose
        return if (originalPredicate === newPredicate) {
            // unchanged
            container
        } else HasContainer(container.key, newPredicate)
        // create a new container with the new predicate
    }

    private fun convertPredicate(predicate: P<*>): P<*> {
        return if (predicate is AndP<*>) {
            convertAndP(predicate)
        } else if (predicate is OrP<*>) {
            convertOrP(predicate)
        } else if (predicate is TextP) {
            convertTextP(predicate)
        } else {
            convertBasicP(predicate)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun convertAndP(predicate: AndP<*>): P<*> {
        val children = predicate.predicates
        val newChildren = mutableListOf<P<*>>()
        var changed = false
        for (child in children) {
            val replacement = convertPredicate(child)
            if (child !== replacement) {
                changed = true
                newChildren.add(replacement)
            } else {
                newChildren.add(child)
            }
        }
        return if (changed) {
            // changed, create a new predicate
            AndP(newChildren as List<P<Any>>)
        } else {
            // unchanged, return the old predicate
            predicate
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun convertOrP(predicate: OrP<*>): P<*> {
        val children = predicate.predicates
        val newChildren = mutableListOf<P<*>>()
        var changed = false
        for (child in children) {
            val replacement = convertPredicate(child)
            if (child !== replacement) {
                changed = true
                newChildren.add(replacement)
            } else {
                newChildren.add(child)
            }
        }
        return if (changed) {
            // changed, create a new predicate
            OrP(newChildren as List<P<Any>>)
        } else {
            // unchanged, return the old predicate
            predicate
        }
    }

    private fun convertTextP(predicate: TextP): P<*> {
        // TextP is always converted into the CP's
        val biPredicate = predicate.biPredicate
        val value = predicate.value
        return when (biPredicate) {
            Text.startingWith -> CP.startsWith(value)
            Text.endingWith -> CP.endsWith(value)
            Text.notStartingWith -> CP.notStartsWith(value)
            Text.notEndingWith -> CP.notEndsWith(value)
            Text.containing -> CP.contains(value)
            Text.notContaining -> CP.notContains(value)
            else -> throw IllegalArgumentException("Encountered unknown BiPredicate in TextP: $biPredicate")
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun convertBasicP(predicate: P<*>): P<*> {
        val biPredicate = predicate.biPredicate
        val value = predicate.value
        if (biPredicate is ChronoCompare
            || biPredicate is ChronoStringCompare
            || biPredicate is DoubleWithinCP
            || biPredicate is DoubleWithoutCP
            || biPredicate is LongWithinCP
            || biPredicate is LongWithoutCP
            || biPredicate is StringWithinCP
            || biPredicate is StringWithoutCP
            || biPredicate is DoubleEqualsCP
            || biPredicate is DoubleNotEqualsCP) {
            return predicate
        }
        return when (biPredicate) {
            Compare.eq -> CP.cEq<Any>(value)
            Compare.neq -> CP.cNeq<Any>(value)
            Compare.gt -> CP.cGt<Any>(value)
            Compare.lt -> CP.cLt<Any>(value)
            Compare.gte -> CP.cGte<Any>(value)
            Compare.lte -> CP.cLte<Any>(value)
            Contains.within -> {
                if (value is Collection<*>) {
                    val collection = value as Collection<Any>
                    CP.cWithin(collection)
                } else {
                    CP.within(value)
                }
            }
            Contains.without -> {
                if (value is Collection<*>) {
                    val collection = value as Collection<Any>
                    CP.cWithout(collection)
                } else {
                    CP.without(value)
                }
            }
            else -> throw IllegalArgumentException("Encountered unknown BiPredicate in basic P: $biPredicate")
        }
    }

}