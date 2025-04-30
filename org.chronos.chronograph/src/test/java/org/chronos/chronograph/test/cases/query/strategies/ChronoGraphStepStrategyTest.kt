package org.chronos.chronograph.test.cases.query.strategies

import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Test
import org.junit.experimental.categories.Category
import strikt.api.expectThat
import strikt.assertions.isEqualTo

@Category(IntegrationTest::class)
class ChronoGraphStepStrategyTest : AllChronoGraphBackendsTest() {

    @Test
    fun canIterateOverEdgesWithIndexOnlyOnVertices() {
        val indexManager = this.graph.indexManagerOnMaster
        // we create an index on VERTICES (but none on edges)
        indexManager.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        indexManager.reindexAll()

        this.graph.tx().createThreadedTx().use { txGraph ->
            var previous = txGraph.addVertex("name", "v0")
            for (i in 1..1000) {
                val vertex = txGraph.addVertex("name", "v[${i}]")
                vertex.addEdge("prev", previous).property("name", "e[${i}]")
                previous = vertex
            }
            txGraph.tx().commit()
        }

        this.graph.tx().createThreadedTx().use { txGraph ->
            val count = txGraph.traversal().E().has("name", CP.startsWith("e")).count().next()
            expectThat(count).isEqualTo(1000)
        }
    }

}