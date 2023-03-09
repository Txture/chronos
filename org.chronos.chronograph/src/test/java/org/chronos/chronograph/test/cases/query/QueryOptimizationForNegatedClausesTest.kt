package org.chronos.chronograph.test.cases.query

import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.containsExactly

class QueryOptimizationForNegatedClausesTest : AllChronoGraphBackendsTest() {

    @Test
    fun canQueryWithNegatedAndNonNegatedClauses() {
        this.graph.tx().createThreadedTx().use { txGraph ->
            txGraph.addVertex(
                "firstName", "John",
                "lastName", "Doe",
                "gender", "male",
            );
            txGraph.addVertex(
                "firstName", "Jane",
                "lastName", "Doe",
                "gender", "female",
            );
            txGraph.addVertex(
                "firstName", "Jack",
                "lastName", "Smith",
                "gender", "male",
            );
            txGraph.addVertex(
                "firstName", "Sarah",
                "lastName", "Smith",
                "gender", "female",
            );

            txGraph.tx().commit()
        }
        graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()
        graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("gender").acrossAllTimestamps().build()
        graph.indexManagerOnMaster.reindexAll()

        this.graph.tx().createThreadedTx().use { txGraph ->
            // the optimizer should be smart enough to figure out that it should:
            // - evaluate "gender == female" on the index
            // - evaluate "lastName != Doe" in-memory on the index result
            // Sadly there's no real way to assert this here at the moment, we can only assert the result...
            val femaleNamesNotDoe = txGraph.traversal().V()
                .has("lastName", CP.neq("Doe"))
                .has("gender", "female")
                .values<String>("firstName")
                .toSet()
            expectThat(femaleNamesNotDoe).containsExactly("Sarah")
        }
    }
}