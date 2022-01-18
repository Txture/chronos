package org.chronos.chronograph.test.cases.index

import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Test
import org.junit.experimental.categories.Category
import strikt.api.expectThat
import strikt.assertions.containsExactly

@Category(IntegrationTest::class)
class MixedIndexingTest : AllChronoGraphBackendsTest() {

    @Test
    fun canIndexVertexPropertyWithDifferentTypesOnDifferentTimeIntervals(){
        val g = this.graph

        val propIndex1 = g.indexManagerOnMaster.create()
            .stringIndex()
            .onVertexProperty("prop")
            .acrossAllTimestamps()
            .build()

        g.indexManagerOnMaster.reindexAll()

        val commit1: Long
        g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(
                T.id, "v1",
                "prop", "one"
            )
            commit1 = txGraph.tx().commitAndReturnTimestamp()
        }
        println("Commit 1: ${commit1}")

        val commit2: Long
        g.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(T.id, "v2")
            commit2 = txGraph.tx().commitAndReturnTimestamp()
        }
        println("Commit 2: ${commit2}")

        Thread.sleep(1)

        g.indexManagerOnMaster.terminateIndex(propIndex1, System.currentTimeMillis())

        g.indexManagerOnMaster.create()
            .doubleIndex()
            .onVertexProperty("prop")
            .withValidityPeriod(System.currentTimeMillis(), Long.MAX_VALUE)
            .build()

        g.indexManagerOnMaster.reindexAll()

        val commit3: Long
        g.tx().createThreadedTx().use { txGraph ->
            txGraph.vertex("v1").property("prop", 3.1415)
            commit3 = txGraph.tx().commitAndReturnTimestamp()
        }
        println("Commit 3: ${commit3}")

        val db = (g as ChronoGraphInternal).backingDB

        val query1 = db.tx(commit1).find()
            .inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX)
            .where("v_prop")
            .isEqualTo("one")
            .unqualifiedKeysAsSet
        expectThat(query1).containsExactly("v1")

        val query2 = db.tx(commit2).find()
            .inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX)
            .where("v_prop")
            .isEqualTo("one")
            .unqualifiedKeysAsSet
        expectThat(query2).containsExactly("v1")

        val query3 = db.tx(commit3).find()
            .inKeyspace(ChronoGraphConstants.KEYSPACE_VERTEX)
            .where("v_prop")
            .isGreaterThan(3.0)
            .unqualifiedKeysAsSet
        expectThat(query3).containsExactly("v1")

    }

}