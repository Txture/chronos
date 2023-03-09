package org.chronos.chronograph.test.cases.query

import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.isTrue

class FailOnExhaustiveIterationTest : AllChronoGraphBackendsTest() {

    @Test
    @FailOnAllEdgesQuery
    fun failsOnAllEdgesQuery() {
        var hasFailed = true
        try {
            this.graph.tx().createThreadedTx().use { tx ->
                tx.traversal().E().toList()
            }
            hasFailed = false
        } catch (expected: Throwable) {
            hasFailed = true
        }
        expectThat(hasFailed).isTrue()
    }

    @Test
    @FailOnAllVerticesQuery
    fun failsOnAllVerticesQuery() {
        var hasFailed = true
        try {
            this.graph.tx().createThreadedTx().use { tx ->
                tx.traversal().V().toList()
            }
            hasFailed = false
        } catch (expected: Throwable) {
            hasFailed = true
        }
        expectThat(hasFailed).isTrue()
    }


}