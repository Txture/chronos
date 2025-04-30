package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.V
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.has
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.inject
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronodb.test.base.InstantiateChronosWith
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphIndexedHasStep
import org.chronos.chronograph.internal.impl.optimizer.step.ChronoGraphPrefetchingBarrierStep
import org.chronos.chronograph.internal.impl.query.ChronoCompare
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery
import org.junit.Assert
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.first
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.map
import strikt.assertions.single

class GremlinIndexedHasQueryTest : AllChronoGraphBackendsTest() {

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.PREFETCH_INDEX_QUERY_MIN_ELEMENTS, value = "0")
    fun canIndexHasQuery() {
        val g = this.getGraph()

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("category").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().open()
        g.addVertex(
            T.id, "s1",
            "name", "Server 1",
            "type", "Server",
            "category", "physical",
        )

        g.addVertex(
            T.id, "s2",
            "name", "Server 2",
            "type", "Server",
            "category", "physical",
        )

        g.addVertex(
            T.id, "s3",
            "name", "Server 3",
            "type", "Server",
            "category", "virtual",
        )

        g.addVertex(
            T.id, "s4",
            "name", "Server 4",
            "type", "Server",
            "category", "cluster",
        )
        g.addVertex(
            T.id, "c1",
            "name", "LAN Connection 1",
            "type", "Connection",
            "category", "physical",
        )
        g.addVertex(
            T.id, "c2",
            "name", "Container Connection 1",
            "type", "Connection",
            "category", "virtual",
        )

        g.tx().commit()

        g.tx().open()

        val vertices = g.traversal()
            .inject("<dummy>")
            .union(
                V<Vertex>().has("category", "virtual"),
                V<Vertex>().has("category", "cluster"),
            ).has("type", CP.eqIgnoreCase("Server"))
            .toSet()

        this.assertNoLoadedEdgesOrVerticesInTransactionContext(g)

        expectThat(vertices) {
            map { it.id() }.containsExactlyInAnyOrder("s3", "s4")
        }
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.PREFETCH_INDEX_QUERY_MIN_ELEMENTS, value = "0")
    fun canIndexHasQueryWithNestedStarts() {
        val g = this.getGraph()

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("category").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().open()
        g.addVertex(
            T.id, "s1",
            "name", "Server 1",
            "type", "Server",
            "category", "physical",
        )

        g.addVertex(
            T.id, "s2",
            "name", "Server 2",
            "type", "Server",
            "category", "physical",
        )

        g.addVertex(
            T.id, "s3",
            "name", "Server 3",
            "type", "Server",
            "category", "virtual",
        )

        g.addVertex(
            T.id, "s4",
            "name", "Server 4",
            "type", "Server",
            "category", "cluster",
        )
        g.addVertex(
            T.id, "c1",
            "name", "LAN Connection 1",
            "type", "Connection",
            "category", "physical",
        )
        g.addVertex(
            T.id, "c2",
            "name", "Container Connection 1",
            "type", "Connection",
            "category", "virtual",
        )
        g.addVertex(
            T.id, "i2",
            "name", "Interface 1",
            "type", "Interface",
            "category", "virtual",
        )

        g.tx().commit()

        g.tx().open()

        val gremlin = g.traversal()
            .inject("<Dummy>")
            .union(
                inject("<Dummy>")
                    .union(
                        V<Vertex>()
                            .has("type", CP.within("Server", "Connection"))
                            .filter(has<String>("category", "physical")),

                        V<Vertex>()
                            .has("type", CP.within("Server", "Connection"))
                            .filter(has<String>("category", "virtual"))
                    )
                    .not(has<String>("type", CP.within("Connection")))
                    .not(has<String>("type", CP.within("Interface")))
            )
            .fold()
            .map { traverser ->
                val vertices = traverser.get()
                vertices.map { it.id().toString() }
            }
            .asAdmin()

        gremlin.applyStrategies()

        println(gremlin)

        expectThat(gremlin.next()).containsExactlyInAnyOrder("s1", "s2", "s3")
        this.assertNoLoadedEdgesOrVerticesInTransactionContext(g)
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.PREFETCH_INDEX_QUERY_MIN_ELEMENTS, value = "0")
    fun canIndexNegatedHasQuery() {
        val g = this.getGraph()

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().longIndex().onVertexProperty("cores").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().open()
        g.addVertex(
            T.id, "s1",
            "name", "Server 1",
            "type", "Server",
            "cores", 2,
        )

        g.addVertex(
            T.id, "s2",
            "name", "Server 2",
            "type", "Server",
            "cores", 4,
        )

        g.addVertex(
            T.id, "s3",
            "name", "Server 3",
            "type", "Server",
            "cores", 6,
        )

        g.addVertex(
            T.id, "s4",
            "name", "Server 4",
            "type", "Server",
            "cores", 8,
        )

        g.addVertex(
            T.id, "s5",
            "name", "Server 5",
            "type", "Server",
            "cores", 10,
        )

        g.addVertex(
            T.id, "s6",
            "name", "Server 6",
            "type", "Server",
            "cores", 12,
        )

        g.addVertex(
            T.id, "v1",
            "name", "VM 1",
            "type", "Server",
        )

        g.addVertex(
            T.id, "v2",
            "name", "VM 2",
            "type", "Server",
        )

        g.tx().commit()

        g.tx().open()

        val gremlin = g.traversal().V()
            .has("type", "Server")
            // this map step prevents rolling up the "not" into the main GraphStep.
            .map{ it.get() }
            .not(has<Int>("cores", CP.gt(4)))
            .id()
            .asAdmin()

        gremlin.applyStrategies()

        println(gremlin)

        // the prefetching barrier should be IN FRONT of the not(...) step, otherwise
        // we drip-feed singular vertices to the prefetcher, and it becomes useless!
        val prefetchingBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(gremlin, ChronoGraphPrefetchingBarrierStep::class.java)
        expectThat(prefetchingBarriers).single().isA<ChronoGraphPrefetchingBarrierStep<*>>().and {
            // the client of the barrier step must be the "has('cores' > 4)" step
            get { this.clients }.single().isA<ChronoGraphIndexedHasStep<*>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("cores")
                    get { this.biPredicate }.isEqualTo(ChronoCompare.GT)
                    get { this.value }.isEqualTo(4)
                }
            }
            // the successor of the barrier must be the "not()" step (the barrier must not be INSIDE the "not()" step!)
            get { this.nextStep }.isA<NotStep<*>>().and {
                get { this.localChildren }.single().isA<GraphTraversal.Admin<Any,Any>>().get { steps }.first().isA<ChronoGraphIndexedHasStep<*>>()
            }
        }

        val serverIdsWithNoMoreThan4Cores = gremlin.toSet()
        expectThat(serverIdsWithNoMoreThan4Cores).containsExactlyInAnyOrder("s1", "s2", "v1", "v2")

        this.assertNoLoadedEdgesOrVerticesInTransactionContext(g)
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.PREFETCH_INDEX_QUERY_MIN_ELEMENTS, value = "0")
    fun canRetainLabelsWhenReplacingHasWithIndexedVersion() {
        val g = this.getGraph()

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().longIndex().onVertexProperty("cores").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().open()
        g.addVertex(
            T.id, "s1",
            "name", "Server 1",
            "type", "Server",
            "cores", 2,
        )

        g.addVertex(
            T.id, "s2",
            "name", "Server 2",
            "type", "Server",
            "cores", 4,
        )

        g.addVertex(
            T.id, "s3",
            "name", "Server 3",
            "type", "Server",
            "cores", 6,
        )

        g.addVertex(
            T.id, "s4",
            "name", "Server 4",
            "type", "Server",
            "cores", 8,
        )

        g.addVertex(
            T.id, "s5",
            "name", "Server 5",
            "type", "Server",
            "cores", 10,
        )

        g.addVertex(
            T.id, "s6",
            "name", "Server 6",
            "type", "Server",
            "cores", 12,
        )

        g.addVertex(
            T.id, "v1",
            "name", "VM 1",
            "type", "Server",
        )

        g.addVertex(
            T.id, "v2",
            "name", "VM 2",
            "type", "Server",
        )

        g.tx().commit()

        g.tx().open()

        val gremlin = g.traversal().V()
            .has("type", "Server")
            // this map step prevents rolling up the "not" into the main GraphStep.
            .map{ it.get() }
            .has("cores", CP.gt(8)).`as`("my-label")
            // the dedup() here doesn't really do anything, but gremlin will complain
            // when the label isn't there.
            .dedup("my-label")
            .id()
            .asAdmin()

        gremlin.applyStrategies()

        println(gremlin)

        // the prefetching barrier should be IN FRONT of the not(...) step, otherwise
        // we drip-feed singular vertices to the prefetcher, and it becomes useless!
        val prefetchingBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(gremlin, ChronoGraphPrefetchingBarrierStep::class.java)
        expectThat(prefetchingBarriers).single().isA<ChronoGraphPrefetchingBarrierStep<*>>().and {
            // the client of the barrier step must be the "has('cores' > 4)" step
            get { this.clients }.single().isA<ChronoGraphIndexedHasStep<*>>().and {
                get { this.hasContainers }.single().and {
                    get { this.key }.isEqualTo("cores")
                    get { this.biPredicate }.isEqualTo(ChronoCompare.GT)
                    get { this.value }.isEqualTo(8)
                }
                // ensure that the label is still there
                get { this.labels }.containsExactlyInAnyOrder("my-label")
            }
        }

        val serverIdsWithMoreThan8Cores = gremlin.toSet()
        expectThat(serverIdsWithMoreThan8Cores).containsExactlyInAnyOrder("s5", "s6")

        this.assertNoLoadedEdgesOrVerticesInTransactionContext(g)
    }

    private fun assertNoLoadedEdgesOrVerticesInTransactionContext(g: ChronoGraph) {
        val ctx = g.tx().currentTransaction.context as GraphTransactionContextInternal
        val loadedVertexIds = ctx.loadedVertexIds.toSet()
        if (loadedVertexIds.isNotEmpty()) {
            Assert.fail("This test asserts that no vertices or edges have been loaded, but there have been loaded vertices: ${loadedVertexIds}")
        }
        val loadedEdgeIds = ctx.loadedEdgeIds.toSet()
        if (loadedEdgeIds.isNotEmpty()) {
            Assert.fail("This test asserts that no vertices or edges have been loaded, but there have been loaded edges: ${loadedEdgeIds}")
        }
        val modifiedElements = ctx.modifiedElements.toSet()
        if (modifiedElements.isNotEmpty()) {
            Assert.fail("This test asserts that no elements have been loaded or modified, but there have been modified elements: ${modifiedElements}")
        }
    }

}