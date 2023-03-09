package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.process.traversal.Compare
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.internal.impl.query.ChronoCompare
import org.chronos.chronograph.internal.impl.util.ChronoGraphStepUtil
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__` as AnonymousTraversal

class HasStepSimplificationTest {

    @Test
    fun canIntersectWithinClauses() {
        val originalFilters = AnonymousTraversal.has<Vertex>("name", CP.cWithin("hello", "world", "foo"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("name", CP.cWithin("hello", "world", "bar"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("name", CP.cWithin("hello", "world", "bar"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("name", CP.cWithin("hello", "world", "baz"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("x", CP.eq("nop")) // unrelated query
            .asAdmin().steps.filterIsInstance<FilterStep<Vertex>>()

        val optimizedFilters = ChronoGraphStepUtil.optimizeFilters(originalFilters)
        expectThat(optimizedFilters).hasSize(2).and {
            one {
                isA<HasStep<Vertex>>().get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("name")
                    get { this.biPredicate }.isEqualTo(ChronoCompare.WITHIN)
                    get { this.predicate.originalValue }.isEqualTo(setOf("hello", "world"))
                }
            }
            one {
                isA<HasStep<Vertex>>().get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("x")
                    get { this.biPredicate }.isEqualTo(Compare.eq)
                    get { this.predicate.originalValue }.isEqualTo("nop")
                }
            }
        }
    }

    @Test
    fun canDetectOverridingClauses(){
        val originalFilters = AnonymousTraversal.has<Vertex>("name", CP.cWithin("hello", "world", "foo"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("name", CP.cWithin("hello", "world", "bar"))
            .out()
            .has("name", CP.cWithin("hello"))
            .out() // we just do this to force gremlin to split the hasContainers, will be ignored
            .has("x", CP.eq("nop"))  // unrelated query
            .asAdmin().steps.filterIsInstance<FilterStep<Vertex>>()

        val optimizedFilters = ChronoGraphStepUtil.optimizeFilters(originalFilters)
        expectThat(optimizedFilters).hasSize(2).and {
            one {
                isA<HasStep<Vertex>>().get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("name")
                    get { this.biPredicate }.isEqualTo(ChronoCompare.EQ)
                    get { this.predicate.originalValue }.isEqualTo("hello")
                }
            }
            one {
                isA<HasStep<Vertex>>().get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("x")
                    get { this.biPredicate }.isEqualTo(Compare.eq)
                    get { this.predicate.originalValue }.isEqualTo("nop")
                }
            }
        }
    }
}