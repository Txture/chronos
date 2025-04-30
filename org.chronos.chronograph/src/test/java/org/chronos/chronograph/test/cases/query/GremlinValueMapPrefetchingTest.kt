package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__` as AnonymousTraversal
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.chronos.chronodb.test.base.InstantiateChronosWith
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.api.structure.ChronoVertex
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration.*
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Assert
import org.junit.Test
import org.junit.experimental.categories.Category
import strikt.api.expectThat
import strikt.assertions.*
import java.util.UUID

@Category(IntegrationTest::class)
class GremlinValueMapPrefetchingTest : AllChronoGraphBackendsTest() {

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun canUseValueMapAsTopLevelQueryWithIndex() {
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("description").acrossAllTimestamps()
            .build()
        this.graph.indexManagerOnMaster.reindexAll()

        this.graph.tx().createThreadedTx().use { tx ->
            val descriptions = listOf("foo", "bar", "baz")
            repeat(1000) {
                tx.addVertex(
                    "name", "v${it}",
                    "description", descriptions[it % descriptions.size]
                )
            }
            tx.tx().commit()
        }

        this.graph.tx().createThreadedTx().use { tx ->
            val gremlinResult = tx.traversal().V()
                // "name" is indexed -> we get lazy vertex proxies
                .has("name", CP.startsWith("v"))
                .valueMap<String>("name", "description") // use the index to answer the query
                .with(WithOptions.tokens, WithOptions.ids)
                .toList()
            expectThat(gremlinResult).hasSize(1000).all {
                get("name").isA<Collection<String>>().single().startsWith("v")
                get("description").isA<Collection<String>>().single().isContainedIn(setOf("foo", "bar", "baz"))
            }
            // ensure that NO vertex was actually loaded from disk
            assertNoLoadedEdgesOrVerticesInTransactionContext(tx)
        }
    }

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun canUseValueMapAsNestedQueryWithIndex() {
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("description").acrossAllTimestamps()
            .build()
        this.graph.indexManagerOnMaster.reindexAll()

        this.graph.tx().createThreadedTx().use { tx ->
            val descriptions = listOf("foo", "bar", "baz")
            repeat(1000) {
                tx.addVertex(
                    "name", "v${it}",
                    "description", descriptions[it % descriptions.size]
                )
            }
            tx.tx().commit()
        }

        this.graph.tx().createThreadedTx().use { tx ->
            val gremlinResult = tx.traversal().V()
                // "name" is indexed -> we get lazy vertex proxies
                .has("name", CP.startsWith("v"))
                .sideEffect(
                    // use the index to answer the query
                    AnonymousTraversal.valueMap<Vertex, String>("name", "description").aggregate("props")
                )
                .cap<Collection<Map<String, Collection<String>>>>("props")
                .next()

            expectThat(gremlinResult).hasSize(1000).all {
                get("name").isA<Collection<String>>().single().startsWith("v")
                get("description").isA<Collection<String>>().single().isContainedIn(setOf("foo", "bar", "baz"))
            }
            // ensure that NO vertex was actually loaded from disk
            assertNoLoadedEdgesOrVerticesInTransactionContext(tx)
        }
    }

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun canUseValueMapAsNestedUnionTraversalWithIndex() {
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("description").acrossAllTimestamps()
            .build()
        this.graph.indexManagerOnMaster.reindexAll()

        this.graph.tx().createThreadedTx().use { tx ->
            val descriptions = listOf("foo", "bar", "baz")
            repeat(1000) {
                tx.addVertex(
                    "name", "v${it}",
                    "description", descriptions[it % descriptions.size]
                )
            }
            tx.tx().commit()
        }

        this.graph.tx().createThreadedTx().use { tx ->
            // here we use the "traversal union" pattern which injects a dummy token (0 in this case)
            // into the pipeline which is then used to trigger a union of traversals.
            val gremlinResult = tx.traversal()
                .inject(0)
                .union(
                    AnonymousTraversal.V<Vertex>()
                        // "name" is indexed -> we get lazy vertex proxies
                        .has("name", CP.startsWith("v"))
                        .sideEffect(
                            // use the index to answer the query
                            AnonymousTraversal.valueMap<Vertex, String>("name", "description").aggregate("props")
                        )
                )
                .cap<Collection<Map<String, Collection<String>>>>("props")
                .next()

            expectThat(gremlinResult).hasSize(1000).all {
                get("name").isA<Collection<String>>().single().startsWith("v")
                get("description").isA<Collection<String>>().single().isContainedIn(setOf("foo", "bar", "baz"))
            }
            // ensure that NO vertex was actually loaded from disk
            assertNoLoadedEdgesOrVerticesInTransactionContext(tx)
        }
    }


    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun onlyRequestedPropertiesAreReturned() {
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build()
        this.graph.indexManagerOnMaster.create().stringIndex().onVertexProperty("description").acrossAllTimestamps()
            .build()
        this.graph.indexManagerOnMaster.reindexAll()

        this.graph.tx().createThreadedTx().use { tx ->
            val descriptions = listOf("foo", "bar", "baz")
            repeat(1000) {
                tx.addVertex(
                    T.id, UUID.randomUUID().toString(),
                    "name", "v${it}",
                    "description", descriptions[it % descriptions.size]
                )
            }
            tx.tx().commit()
        }

        this.graph.tx().createThreadedTx().use { tx ->
            // here we use the "traversal union" pattern which injects a dummy token (0 in this case)
            // into the pipeline which is then used to trigger a union of traversals.
            val gremlinResult = tx.traversal()
                .inject(0)
                .union(
                    AnonymousTraversal.V<Vertex>()
                        // "name" is indexed -> we get lazy vertex proxies
                        .has("name", CP.startsWith("v"))
                        .sideEffect(
                            // use the index to answer the query
                            AnonymousTraversal.valueMap<Vertex, String>("name")
                                .with(WithOptions.tokens, WithOptions.ids).aggregate("props")
                        )
                )
                .cap<Collection<Map<Any, Collection<String>>>>("props")
                .next()

            expectThat(gremlinResult).hasSize(1000).all {
                keys.containsExactlyInAnyOrder("name", T.id)
                get("name").isA<Collection<String>>().single().startsWith("v")
                get(T.id).isA<String>().get { parseUUIDOrNull(this) }.isNotNull()
            }
            // ensure that NO vertex was actually loaded from disk
            assertNoLoadedEdgesOrVerticesInTransactionContext(tx)
        }
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


    private fun parseUUIDOrNull(maybeUUID: String): UUID? {
        return try {
            UUID.fromString(maybeUUID)
        } catch (_: Exception) {
            null
        }
    }
}