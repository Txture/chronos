package org.chronos.chronograph.test.cases.query.strategies

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__` as AnonymousTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class EarlyLimitStrategyTest: AllChronoGraphBackendsTest() {


    @Test
    fun canPerformMultipleLimitZeroInOneQuery(){
        val result = this.graph.traversal().inject(0).union(
            AnonymousTraversal.V<Vertex>()
                .has("kind", "entity")
                .limit(0) // double ".limit(0)" caused issues for the optimizer in some cases.
                .limit(0)
                .fold()
                .sideEffect(
                    AnonymousTraversal.unfold<Vertex>()
                        .valueMap<String>("type").with(WithOptions.tokens, WithOptions.ids)
                        .aggregate("groupByType")
                )
                .unfold<Vertex>()
                .aggregate("result")
                .cap<Map<String, Any>>("groupByType", "result")
                .map { traverser ->
                    val map = traverser.get()
                    map.size
                }
        ).next()

        expectThat(result).isEqualTo(2)
    }


}