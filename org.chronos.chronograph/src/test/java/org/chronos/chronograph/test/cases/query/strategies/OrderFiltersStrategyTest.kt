package org.chronos.chronograph.test.cases.query.strategies

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.out
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.internal.impl.optimizer.strategy.OrderFiltersStrategy
import org.chronos.chronograph.internal.impl.optimizer.strategy.OrderFiltersStrategy.eliminateDuplicateHasSteps
import org.chronos.chronograph.internal.impl.optimizer.strategy.OrderFiltersStrategy.findReorderableSteps
import org.chronos.chronograph.internal.impl.optimizer.strategy.OrderFiltersStrategy.flattenAndP
import org.chronos.chronograph.internal.impl.optimizer.strategy.OrderFiltersStrategy.unwrapAndP
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.*

@Category(IntegrationTest::class)
class OrderFiltersStrategyTest {

    @Test
    fun canDetectPredicateEquality() {
        expect {
            that(P.eq("hello")).isEqualTo(P.eq("hello"))
            that(P.eq("hello")).isNotEqualTo(P.eq("world"))
            that(P.gt(2)).isEqualTo(P.gt(2))
            that(P.gt(2)).isNotEqualTo(P.eq(2))
            that(TextP.containing("foo")).isEqualTo(TextP.containing("foo"))
            that(TextP.containing("foo")).isNotEqualTo(TextP.containing("bar"))
            that(P.gt(2).and(P.lt(5))).isEqualTo(P.gt(2).and(P.lt(5)))
            that(P.gt(2).and(P.lt(5))).isNotEqualTo(P.gt(2).and(P.lt(6)))
            that(P.gt(2).or(P.lt(5))).isEqualTo(P.gt(2).or(P.lt(5)))
            that(P.gt(2).or(P.lt(5))).isNotEqualTo(P.gt(2).or(P.lt(6)))
            that(P.gt(2).and(P.lt(5))).isNotEqualTo(P.gt(2).or(P.lt(5)))
            that(P.gt(2).or(P.lt(5))).isNotEqualTo(P.gt(2).and(P.lt(5)))
            that(P.gt(2).or(P.lt(5)).or(P.lt(10))).isNotEqualTo(P.gt(2).or(P.lt(5)))
            that(CP.matchesRegex(".*")).isEqualTo(CP.matchesRegex(".*"))
            that(CP.matchesRegex(".*")).isNotEqualTo(CP.matchesRegex("a-z"))
            that(CP.within("foo", "bar", "baz")).isEqualTo(CP.within("foo", "bar", "baz"))
            that(CP.within("foo", "bar", "baz")).isNotEqualTo(CP.within("foo", "bar", "lol"))
            that(CP.within("foo", "bar", "baz")).isNotEqualTo(CP.within("foo", "bar"))
            that(CP.withinIgnoreCase("foo", "bar", "baz")).isEqualTo(CP.withinIgnoreCase("foo", "bar", "baz"))
            that(CP.withinIgnoreCase("foo", "bar", "baz")).isNotEqualTo(CP.withinIgnoreCase("foo", "bar", "lol"))
            that(CP.withinIgnoreCase("foo", "bar", "baz")).isNotEqualTo(CP.withinIgnoreCase("foo", "bar"))
            that(CP.without("foo", "bar", "baz")).isEqualTo(CP.without("foo", "bar", "baz"))
            that(CP.without("foo", "bar", "baz")).isNotEqualTo(CP.without("foo", "bar", "lol"))
            that(CP.without("foo", "bar", "baz")).isNotEqualTo(CP.without("foo", "bar"))
            that(CP.withoutIgnoreCase("foo", "bar", "baz")).isEqualTo(CP.withoutIgnoreCase("foo", "bar", "baz"))
            that(CP.withoutIgnoreCase("foo", "bar", "baz")).isEqualTo(CP.withoutIgnoreCase("BAR", "BAZ", "FOO"))
            that(CP.withoutIgnoreCase("foo", "bar", "baz")).isNotEqualTo(CP.withoutIgnoreCase("foo", "bar", "lol"))
            that(CP.withoutIgnoreCase("foo", "bar", "baz")).isNotEqualTo(CP.withoutIgnoreCase("foo", "bar"))
        }
    }

    @Test
    fun canFlattenAndP() {
        expectThat(null) {
            get { flattenAndP(P.lt(3).and(P.lt(5).and(P.lt(7))) as AndP) }.containsExactlyInAnyOrder(P.lt(3), P.lt(5), P.lt(7))
            get { flattenAndP(P.lt(3).and(P.lt(5)).and(P.lt(7)) as AndP) }.containsExactlyInAnyOrder(P.lt(3), P.lt(5), P.lt(7))
        }
    }

    @Test
    fun canUnwrapAndP() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>().V()
            // this "has" should be unwrapped
            .has("x", P.gt(3).and(P.lt(10).and(P.neq(5))))
            .has("y", P.eq(4))
            .asAdmin()
        val stepsBefore = TraversalHelper.getStepsOfClass(HasStep::class.java, traversal)
        unwrapAndP(stepsBefore)
        val hasContainersAfter = TraversalHelper.getStepsOfClass(HasStep::class.java, traversal).flatMap { it.hasContainers }
        expectThat(hasContainersAfter).hasSize(4).and {
            one {
                get { this.key }.isEqualTo("x")
                get { this.predicate }.isEqualTo(P.gt(3))
            }
            one {
                get { this.key }.isEqualTo("x")
                get { this.predicate }.isEqualTo(P.lt(10))
            }
            one {
                get { this.key }.isEqualTo("x")
                get { this.predicate }.isEqualTo(P.neq(5))
            }
            one {
                get { this.key }.isEqualTo("y")
                get { this.predicate }.isEqualTo(P.eq(4))
            }
        }
    }

    @Test
    fun canDetectReorderableSteps() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>()
            .V()
            // the following steps can be reordered
            .has("x", P.gt(3).and(P.lt(10).and(P.neq(5))))
            .filter(out("foo").has("bar"))
            .has("y", P.eq(4))
            // "out" is not reorderable and blocks off everything after it
            .out("myLabel")
            .has("z", 5)
            .asAdmin()
        val reorderableSteps = findReorderableSteps(traversal.startStep as GraphStep<*, *>)
        expectThat(reorderableSteps).hasSize(3).and {
            one {
                isA<HasStep<*>>().get { this.hasContainers }.all {
                    get { this.key }.isEqualTo("x")
                }
            }
            one {
                isA<HasStep<*>>().get { this.hasContainers }.all {
                    get { this.key }.isEqualTo("y")
                }
            }
            one {
                isA<TraversalFilterStep<*>>()
            }
        }
    }

    @Test
    fun reorderingWillNotStepOverLabels() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>()
            .V()
            // the following steps can be reordered
            .has("x", P.gt(3).and(P.lt(10).and(P.neq(5))))
            .has("y", P.eq(4))
            // steps with labels on them cannot be reordered and block
            // off all steps that come afterwards
            .filter(out("blah")).`as`("boom")
            // as the previous step has a label, none of the following may be reordered
            .filter(out("foo").has("bar"))
            .has("alpha", 3)
            .asAdmin()
        val reorderableSteps = findReorderableSteps(traversal.startStep as GraphStep<*, *>)
        expectThat(reorderableSteps).single()
            .isA<HasStep<*>>()
            .get { this.hasContainers.asSequence().map { it.key }.toSet() }.containsExactlyInAnyOrder("x", "y")
    }

    @Test
    fun reorderingWillNotStepOverLambdas() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>()
            .V()
            // the following steps can be reordered
            .has("x", P.gt(3).and(P.lt(10).and(P.neq(5))))
            .has("y", P.eq(4))
            // don't try to reorder a lambda step, and stop immediately
            .filter { true }
            // as the previous step has a lambda, none of the following may be reordered
            .filter(out("foo").has("bar"))
            .has("alpha", 3)
            .asAdmin()
        val reorderableSteps = findReorderableSteps(traversal.startStep as GraphStep<*, *>)
        expectThat(reorderableSteps).single()
            .isA<HasStep<*>>()
            .get { this.hasContainers.asSequence().map { it.key }.toSet() }.containsExactlyInAnyOrder("x", "y")
    }

    @Test
    fun canEliminateDuplicateSteps() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>()
            .V()
            // the following steps can be reordered
            .has("x", P.eq("foo"))
            .asAdmin()
        // we cannot use ".has" twice because the builder method would immediately eliminate the
        // duplicate. However, we need to test this case anyways because the duplicate may be a
        // result of reordering, in which case the builder can do nothing.
        traversal.addStep(HasStep<Vertex>(traversal, HasContainer("x", P.eq("foo"))))

        val hasSteps = TraversalHelper.getStepsOfClass(HasStep::class.java, traversal)
        expectThat(hasSteps).hasSize(2).all {
            get { this.hasContainers.single().key }.isEqualTo("x")
            get { this.hasContainers.single().predicate }.isEqualTo(P.eq("foo"))
        }
        eliminateDuplicateHasSteps(traversal, hasSteps)

        val hasStepsAfter = TraversalHelper.getStepsOfClass(HasStep::class.java, traversal)
        expectThat(hasStepsAfter).single().and {
            get { this.hasContainers.single().key }.isEqualTo("x")
            get { this.hasContainers.single().predicate }.isEqualTo(P.eq("foo"))
        }
    }

    @Test
    fun canEliminateDuplicateSteps2() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>().V()
            .has("name", "marko")
            .and()
            .has("name", "marko")
            .and()
            .has("name", "marko")
            .asAdmin()
        OrderFiltersStrategy.optimize(traversal, emptySet())

        expectThat(traversal.steps).hasSize(2).and {
            get(0).isA<GraphStep<Vertex, Vertex>>()
            get(1).isA<HasStep<Vertex>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("name")
                    get { this.predicate }.isEqualTo(P.eq("marko"))
                }
            }
        }
    }

    @Test
    fun canOptimizeQuery() {
        val traversal = DefaultGraphTraversal<Vertex, Vertex>()
            .V()
            .has("kind", "entity")
            .filter(out("foo1"))
            .has("aScore", CP.gt(90.0))
            .filter(out("foo2"))
            .has("name", CP.contains("server"))
            .filter(out("foo3"))
            .has("kind", "entity")
            .filter { true }
            .has("x", "y")
            .asAdmin()
        OrderFiltersStrategy.optimize(traversal, setOf("kind", "name"))

        expectThat(traversal.steps).hasSize(9).and {
            get(0).isA<GraphStep<Vertex, Vertex>>()
            get(1).isA<HasStep<*>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("kind")
                    get { this.predicate }.isEqualTo(P.eq("entity"))
                }
            }
            get(2).isA<HasStep<*>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("name")
                    get { this.predicate }.isEqualTo(CP.contains("server"))
                }
            }
            get(3).isA<HasStep<*>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("aScore")
                    get { this.predicate }.isEqualTo(CP.gt(90.0))
                }
            }
            get(4).isA<TraversalFilterStep<*>>()
            get(5).isA<TraversalFilterStep<*>>()
            get(6).isA<TraversalFilterStep<*>>()
            get(7).isA<LambdaFilterStep<*>>()
            get(8).isA<HasStep<*>>()
        }

    }

}