package org.chronos.chronograph.internal.impl.util

import com.google.common.collect.Sets
import org.apache.tinkerpop.gremlin.process.traversal.*
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.api.builder.query.DoubleWithoutCP
import org.chronos.chronograph.api.builder.query.LongWithoutCP
import org.chronos.chronograph.api.builder.query.StringWithoutCP
import org.chronos.chronograph.api.index.ChronoGraphIndex
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphStep
import org.chronos.chronograph.internal.impl.query.ChronoCompare
import org.chronos.chronograph.internal.impl.query.ChronoStringCompare
import java.util.*
import java.util.function.BiPredicate
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__` as AnonymousTraversal

object ChronoGraphStepUtil {

    private val NEGATED_PREDICATES = Collections.unmodifiableSet(
        setOf(
            Compare.neq,
            Contains.without,
            Text.notStartingWith,
            Text.notEndingWith,
            Text.notContaining,
            ChronoCompare.NEQ,
            ChronoCompare.WITHOUT,
            ChronoStringCompare.STRING_NOT_STARTS_WITH,
            ChronoStringCompare.STRING_NOT_STARTS_WITH_IGNORE_CASE,
            ChronoStringCompare.STRING_NOT_ENDS_WITH,
            ChronoStringCompare.STRING_NOT_ENDS_WITH_IGNORE_CASE,
            ChronoStringCompare.STRING_NOT_CONTAINS,
            ChronoStringCompare.STRING_NOT_CONTAINS_IGNORE_CASE,
            ChronoStringCompare.STRING_NOT_EQUALS_IGNORE_CASE,
            ChronoStringCompare.STRING_NOT_MATCHES_REGEX,
            ChronoStringCompare.STRING_NOT_MATCHES_REGEX_IGNORE_CASE
        )
    )

    @JvmStatic
    fun getIndicesAndKeyspace(
        traversal: Traversal.Admin<*, *>,
        traversers: List<Traverser.Admin<Element>>,
        propertyKeys: Set<String>,
    ): Pair<Set<ChronoGraphIndex>, String>? {
        val types = mutableSetOf<Class<out Any>>()
        val primaryKeys = HashSet<String>(traversers.size)
        for (traverser in traversers) {
            val element = traverser.get()
            types += when (element) {
                is Vertex -> Vertex::class.java
                is Edge -> Edge::class.java
                else -> return null // unknown type, not indexable.
            }
            if (types.size > 1) {
                // can't use index scan for mixed inputs
                return null
            }
            primaryKeys += element.id() as String
        }
        val graph = traversal.graph.orElse(null) as ChronoGraphInternal
        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)
        return if (Vertex::class.java in types) {
            // all elements are vertices. Are the properties indexed?
            if (this.areAllPropertiesIndexed(traversal, Vertex::class.java, propertyKeys)) {
                val vertexPropertyIndices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedVertexPropertiesAtTimestamp(tx.timestamp)
                Pair(vertexPropertyIndices, ChronoGraphConstants.KEYSPACE_VERTEX)
            } else {
                // not all properties are indexed -> can't use index scan
                return null
            }
        } else if (Edge::class.java in types) {
            // all elements are edges. Are the properties indexed?
            if (this.areAllPropertiesIndexed(traversal, Edge::class.java, propertyKeys)) {
                val edgePropertyIndices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedEdgePropertiesAtTimestamp(tx.timestamp)
                Pair(edgePropertyIndices, ChronoGraphConstants.KEYSPACE_EDGE)
            } else {
                // not all properties are indexed -> can't use index scan
                return null
            }
        } else {
            // we're dealing with some unknown input type -> can't use index scan
            return null
        }
    }

    private fun areAllPropertiesIndexed(traversal: Traversal.Admin<*, *>, type: Class<out Element>, propertyKeys: Set<String>): Boolean {
        val graph = traversal.graph.orElse(null) as? ChronoGraph?
            ?: return false

        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)

        val indexManager = graph.getIndexManagerOnBranch(tx.branchName)
        val indices = when (type) {
            Vertex::class.java -> indexManager.getIndexedVertexPropertiesAtTimestamp(tx.timestamp)
            Edge::class.java -> indexManager.getIndexedEdgePropertiesAtTimestamp(tx.timestamp)
            else -> emptySet()
        }
        // ignore dirty indices, they're useless for our purposes.
        val indexedPropertyNames = indices.asSequence()
            .filterNot(ChronoGraphIndex::isDirty)
            .map(ChronoGraphIndex::getIndexedProperty)
            .toSet()
        for (propertyKey in propertyKeys) {
            if (propertyKey !in indexedPropertyNames) {
                return false
            }
        }
        return true
    }


    @JvmStatic
    @Suppress("UNCHECKED_CAST")
    fun <T : Element> optimizeFilters(filters: List<FilterStep<T>>): List<FilterStep<T>> {
        val hasSteps = mutableListOf<HasStep<T>>()
        val outputConditions = mutableListOf<FilterStep<T>>()
        for (filterStep in filters) {
            if (filterStep is HasStep<T>) {
                if (filterStep.hasContainers.size == 1) {
                    hasSteps.add(filterStep)
                    continue
                }
            }
            outputConditions.add(filterStep)
        }

        // we ensured earlier that there is exactly one HasContainer per step.
        val propertyKeyToHasContainers = hasSteps.groupBy { it.hasContainers.single().key }
        for ((_, hasContainers) in propertyKeyToHasContainers) {
            val conditions = hasContainers.toMutableList()
            if (conditions.size <= 1) {
                // only a single condition for the property -> no room for optimization.
                outputConditions.addAll(conditions)
                continue
            }

            // here we have the situation that we have multiple has(...) conditions with the same key.
            // For example:
            // - has("x", CP.within("hello", "world", "foo")
            // - has("x", CP.within("hello", "world", "bar")
            // - has("x", CP.eq("hello"))
            // ... simplifies to just has("x", CP.eq("hello"))

            // the cases for simplification are:
            // - one condition OVERRULES another (within[a,b] overrules within[a,b,c], eq[a] overrules within[a,b])
            // - one condition INTERSECTS with another, producing a result (within[a,b] intersected with within[b,c] produces eq[b])
            var changed = true
            // apply a maximum of optimization steps to avoid endless loops in case of bugs
            val maxSteps = 1000
            var steps = 0
            while (changed && steps < maxSteps) {
                changed = applyOptimizationStep(conditions as MutableList<HasStep<*>>)
                steps++
            }
            outputConditions.addAll(conditions)
        }

        // finally, replace all "within[x]" clauses by "eq[x]"
        outputConditions.replaceAll { originalFilter ->
            if (originalFilter !is HasStep) {
                return@replaceAll originalFilter
            }
            if (originalFilter.hasContainers.size != 1) {
                return@replaceAll originalFilter
            }
            val hasContainer = originalFilter.hasContainers.single()
            if (hasContainer.biPredicate == Compare.eq || hasContainer.biPredicate == ChronoCompare.EQ) {
                return@replaceAll originalFilter
            }
            if (!isWithinCondition(hasContainer)) {
                return@replaceAll originalFilter
            }
            val withinSet = getWithinSet(hasContainer)
            if (withinSet.size != 1) {
                return@replaceAll originalFilter
            }
            // replace within[x] by eq[x]
            HasStep(
                AnonymousTraversal.start<Element>().asAdmin(),
                HasContainer(hasContainer.key, CP.cEq<Any>(withinSet.single()))
            )
        }

        return outputConditions
    }

    private fun applyOptimizationStep(conditions: MutableList<HasStep<*>>): Boolean {
        for (first in conditions.toList()) {
            for (second in conditions) {
                if (first === second) {
                    // same condition, don't do anything
                    continue
                }
                if (overrules(first, second)) {
                    // the second condiition is useless
                    conditions.remove(second)
                    return true
                }
                // compute the intersection filter
                val intersectionStep = getIntersection(first, second)
                if (intersectionStep != null) {
                    conditions.remove(first)
                    conditions.remove(second)
                    conditions.add(intersectionStep)
                    return true
                }
            }
        }
        return false
    }

    private fun overrules(first: HasStep<*>, second: HasStep<*>): Boolean {
        val firstContainer = first.hasContainers.single()
        val secondContainer = second.hasContainers.single()
        if (isWithinCondition(firstContainer) && isWithinCondition(secondContainer)) {
            val firstWithin = getWithinSet(firstContainer)
            val secondWithin = getWithinSet(secondContainer)
            if (secondWithin.containsAll(firstWithin)) {
                // the second condition contains all values of the first, example:
                // - firstWithin: [a,b]
                // - secondWithin: [a,b,c]
                // -> first condition overrules the second one.
                return true
            }
        } else if (isEquals(firstContainer) && isEquals(secondContainer)) {
            val firstValue = firstContainer.predicate.originalValue
            val secondValue = secondContainer.predicate.originalValue
            if (firstValue == secondValue) {
                // the conditions are the same (example: eq[x], eq[x]),
                // so we only need one of these conditions. We can say
                // that the first condition overrules the second one.
                return true
            }
        }
        return false
    }

    private fun isWithinCondition(hasContainer: HasContainer): Boolean {
        val predicate = hasContainer.biPredicate
        // we treat "equals" as "within" with a single element here.
        return predicate == ChronoCompare.WITHIN ||
            predicate == Contains.within ||
            predicate == ChronoCompare.EQ ||
            predicate == Compare.eq
    }

    private fun getWithinSet(hasContainer: HasContainer): Set<*> {
        if (hasContainer.biPredicate == ChronoCompare.WITHIN) {
            val chronoPredicate = hasContainer.predicate as CP<*>
            return Sets.newHashSet(chronoPredicate.originalValue as Collection<*>)
        }
        if (hasContainer.biPredicate == Contains.within) {
            val predicate = hasContainer.predicate
            return Sets.newHashSet(predicate.originalValue as Collection<*>)
        }
        if (hasContainer.biPredicate == ChronoCompare.EQ) {
            return setOf(hasContainer.predicate.originalValue)
        }
        if (hasContainer.biPredicate == Compare.eq) {
            return setOf(hasContainer.predicate.originalValue)
        }
        throw IllegalArgumentException(
            "Cannot get 'within' set from condition which doesn't use the 'within' predicate!" +
                " Predicate is: '" + hasContainer.biPredicate + "'"
        )
    }

    private fun isEquals(hasContainer: HasContainer): Boolean {
        if (hasContainer.biPredicate === ChronoCompare.EQ) {
            return true
        }
        if (hasContainer.biPredicate === Compare.eq) {
            return true
        }

        return false
    }


    private fun getIntersection(first: HasStep<*>, second: HasStep<*>): HasStep<*>? {
        val firstContainer = first.hasContainers.single()
        val secondContainer = second.hasContainers.single()
        if (isWithinCondition(firstContainer) && isWithinCondition(secondContainer)) {
            // we have "within[a,b,c]" and "within[x,y,z]", check if there's an overlap
            val firstWithinSet = getWithinSet(firstContainer)
            val secondWithinSet = getWithinSet(secondContainer)
            val intersection = firstWithinSet.intersect(secondWithinSet)
            return if (intersection.isNotEmpty()) {
                // there's an overlap in the sets. Create a replacement for both.
                HasStep<Element>(
                    AnonymousTraversal.start<Element>().asAdmin(),
                    HasContainer(firstContainer.key, CP.cWithin(intersection))
                )
            } else {
                // TODO: we inferred here that the conditions contradict each other and the query result will be empty.
                // Sadly, we have no way to express this properly at the moment.
                null
            }
        }

        return null
    }

    @JvmStatic
    fun isNegated(filterStep: FilterStep<*>): Boolean {
        when (filterStep) {
            is NotStep<*> -> {
                // note: we COULD check for double-negations here...
                return true
            }

            is ConnectiveStep<*> -> {
                val children: List<Traversal.Admin<*, *>> = filterStep.localChildren
                for (child in children) {
                    if (child is FilterStep<*>) {
                        if (isNegated(child as FilterStep<*>)) {
                            return true
                        }
                    }
                }
            }

            is HasStep<*> -> {
                for (container in filterStep.hasContainers) {
                    when (container.biPredicate) {
                        in NEGATED_PREDICATES -> return true
                        is DoubleWithoutCP -> return true
                        is LongWithoutCP -> return true
                        is StringWithoutCP -> return true
                    }
                }
            }
        }
        return false
    }
}