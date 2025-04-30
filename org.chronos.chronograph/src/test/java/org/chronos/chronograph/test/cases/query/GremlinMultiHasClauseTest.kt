package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.V
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Assert
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(IntegrationTest::class)
class GremlinMultiHasClauseTest : AllChronoGraphBackendsTest() {

    @Test
    fun canExecuteQuery() {
        // the important thing here is not the result (the graph is empty anyway) but
        // to ensure that the optimizer doesn't choke on the query. Do not worry about
        // the semantics, it is purely artificial to trigger corner cases in the optimizer.
        val result = this.graph.traversal()
            .inject("<dummy>")
            .union(
                V<Vertex>()
                    .has("entityClass", CP.within("t:18202271-f488-4e43-8226-d985c46d0c66"))
                    .has("kind", "entity")
                    .limit(0)
                    .has("entityClass", CP.within("t:18202271-f488-4e43-8226-d985c46d0c66"))
                    .has("entityClass", CP.within("t:18202271-f488-4e43-8226-d985c46d0c66"))
                    .has("entityClass", CP.within("t:18202271-f488-4e43-8226-d985c46d0c66"))
                    .fold()
                    .map { it.get().size }
            )
            .next()
        Assert.assertEquals(0, result)
    }


}