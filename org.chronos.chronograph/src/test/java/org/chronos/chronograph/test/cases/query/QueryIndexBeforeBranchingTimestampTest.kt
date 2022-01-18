package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Test
import org.junit.experimental.categories.Category
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder

@Category(IntegrationTest::class)
class QueryIndexBeforeBranchingTimestampTest : AllChronoGraphBackendsTest() {

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    fun canQuerySecondaryIndexBeforeBranchingTimestamp(){
        val g = this.graph
        g.indexManagerOnMaster.create()
            .stringIndex()
            .onVertexProperty("name")
            .acrossAllTimestamps()
            .assumeNoPriorValues(true)
            .build()


        val commit1 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(
                T.id, "v1",
                "name", "John Doe"
            )
            txGraph.addVertex(
                T.id, "v2",
                "name", "Jane Doe"
            )
            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit2 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(
                T.id, "v3",
                "name", "Jack Smith"
            )
            txGraph.tx().commitAndReturnTimestamp()
        }

        g.branchManager.createBranch("MyBranch")

        val commit3 = g.tx().createThreadedTx("MyBranch").use { txGraph ->
            txGraph.addVertex(
                T.id, "v4",
                "name", "Sarah Doe"
            )
            txGraph.vertex("v2").remove()
            txGraph.tx().commitAndReturnTimestamp()
        }

        val commit4 = g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(
                T.id, "v5",
                "name", "Jack Frost"
            )
            txGraph.tx().commitAndReturnTimestamp()
        }

        println("Commit 1: ${commit1}")
        println("Commit 2: ${commit2}")
        println("Commit 3: ${commit3}")
        println("Commit 4: ${commit4}")


        g.tx().createThreadedTx("MyBranch", commit1).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("Doe")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2")
        }

        g.tx().createThreadedTx("MyBranch", commit2).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("J")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2", "v3")
        }

        g.tx().createThreadedTx("MyBranch", commit3).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("Doe")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v4")
        }

        g.tx().createThreadedTx(commit1).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("Doe")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2")
        }

        g.tx().createThreadedTx(commit2).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("J")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2", "v3")
        }

        g.tx().createThreadedTx(commit3).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("J")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2", "v3")
        }

        g.tx().createThreadedTx(commit4).use { txGraph ->
            val vertexIds = txGraph.traversal().V().has("name", CP.contains("J")).id().toSet()
            expectThat(vertexIds).containsExactlyInAnyOrder("v1", "v2", "v3", "v5")
        }
    }


}