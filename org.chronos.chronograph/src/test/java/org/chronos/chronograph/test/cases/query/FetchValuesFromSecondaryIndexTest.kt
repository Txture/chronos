package org.chronos.chronograph.test.cases.query

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronodb.api.NullSortPosition
import org.chronos.chronodb.test.base.InstantiateChronosWith
import org.chronos.chronograph.api.builder.query.ordering.COrder
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery
import org.junit.Assert.fail
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.*

class FetchValuesFromSecondaryIndexTest : AllChronoGraphBackendsTest() {

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    fun valuesQueryWorks() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            "firstName", "John",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            "firstName", "Jane",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            "firstName", "Jack",
            "lastName", "Smith"
        )
        txGraph.tx().commit()

        val names = g.traversal().V().values<String>("firstName").toList()
        expectThat(names).containsExactlyInAnyOrder("John", "Jane", "Jack")

        val names2 = g.traversal().V().values<String>("firstName", "lastName").toList()
        expectThat(names2).containsExactlyInAnyOrder("John", "Jane", "Jack", "Doe", "Doe", "Smith")

        val valueMap1 = g.traversal().V().valueMap<Any>("firstName").with(WithOptions.tokens, WithOptions.ids).toList()
        println(valueMap1)
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    fun valuesQueryWorksWithMultiValuesWithIndex() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            "firstName", listOf("John1", "John2"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            "firstName", listOf("Jane1", "Jane2"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            "firstName", listOf("Jack"),
            "lastName", listOf("Smith")
        )
        txGraph.tx().commit()

        val names = g.traversal().V().values<String>("firstName").toList()
        expectThat(names).containsExactlyInAnyOrder("John1", "John2", "Jane1", "Jane2", "Jack")

        val names2 = g.traversal().V().values<String>("firstName", "lastName").toList()
        expectThat(names2).containsExactlyInAnyOrder("John1", "John2", "Jane1", "Jane2", "Jack", "Doe", "Doe", "Smith")
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    fun valuesQueryWorksWithMultiValuesWithoutIndex() {
        val g = this.graph

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            "firstName", listOf("John1", "John2"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            "firstName", listOf("Jane1", "Jane2"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            "firstName", listOf("Jack"),
            "lastName", listOf("Smith")
        )
        txGraph.tx().commit()

        val names = g.traversal().V().values<String>("firstName").toList()
        expectThat(names).containsExactlyInAnyOrder("John1", "John2", "Jane1", "Jane2", "Jack")

        val names2 = g.traversal().V().values<String>("firstName", "lastName").toList()
        expectThat(names2).containsExactlyInAnyOrder("John1", "John2", "Jane1", "Jane2", "Jack", "Doe", "Doe", "Smith")
    }


    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun valueMapQueryWorks() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "firstName", "John",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            T.id, "p2",
            "firstName", "Jane",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            T.id, "p3",
            "firstName", "Jack",
            "lastName", "Smith"
        )
        txGraph.tx().commit()

        val valueMap = g.traversal().V().valueMap<Any>("firstName", "lastName").with(WithOptions.tokens, WithOptions.ids).toList()
        expectThat(valueMap) {
            one {
                get { this[T.id] }.isEqualTo("p1")
                get { this["firstName"] }.isA<List<String>>().containsExactly("John")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            one {
                get { this[T.id] }.isEqualTo("p2")
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jane")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            one {
                get { this[T.id] }.isEqualTo("p3")
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jack")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Smith")
            }
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun valueMapQueryWorksOnMultiValuesWithIndex() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "firstName", listOf("John"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            T.id, "p2",
            "firstName", listOf("Jane"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            T.id, "p3",
            "firstName", listOf("Jack"),
            "lastName", listOf("Smith")
        )
        txGraph.addVertex(
            T.id, "p4",
            "firstName", emptyList<String>()
        )
        txGraph.tx().commit()

        val valueMap = g.traversal().V().valueMap<Any>("firstName", "lastName").with(WithOptions.tokens, WithOptions.ids).toList()
        expectThat(valueMap).hasSize(4).and {
            get { this.firstOrNull { it[T.id] == "p1" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("John")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            get { this.firstOrNull { it[T.id] == "p2" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jane")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            get { this.firstOrNull { it[T.id] == "p3" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jack")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Smith")
            }
            get { this.firstOrNull { it[T.id] == "p4" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().isEmpty()
                get { this["lastName"] }.isA<List<String>>().isEmpty()
            }
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun valueMapQueryWorksOnMultiValuesWithoutIndex() {
        val g = this.graph
        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "firstName", listOf("John"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            T.id, "p2",
            "firstName", listOf("Jane"),
            "lastName", listOf("Doe")
        )
        txGraph.addVertex(
            T.id, "p3",
            "firstName", listOf("Jack"),
            "lastName", listOf("Smith")
        )
        txGraph.addVertex(
            T.id, "p4",
            "firstName", emptyList<String>()
        )
        txGraph.tx().commit()

        val valueMap = g.traversal().V().valueMap<Any>("firstName", "lastName").with(WithOptions.tokens, WithOptions.ids).toList()
        expectThat(valueMap).hasSize(4).and {
            get { this.firstOrNull { it[T.id] == "p1" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("John")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            get { this.firstOrNull { it[T.id] == "p2" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jane")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
            }
            get { this.firstOrNull { it[T.id] == "p3" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().containsExactly("Jack")
                get { this["lastName"] }.isA<List<String>>().containsExactly("Smith")
            }
            get { this.firstOrNull { it[T.id] == "p4" } }.isNotNull().and {
                get { this["firstName"] }.isA<List<String>>().isEmpty()
                get { this["lastName"] }.isA<List<String>>().isEmpty()
            }
        }
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun orderByWithIndexWorks() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "type", "Person",
            "firstName", "John",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            T.id, "p2",
            "type", "Person",
            "firstName", "Jane",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            T.id, "p3",
            "type", "Person",
            "firstName", "Jack",
            "lastName", "Smith"
        )
        txGraph.tx().commit()

        val result = g.traversal().V()
            .has("type", "Person")
            .order().by("lastName", Order.asc).by("firstName", Order.desc)
            .valueMap<List<String>>("firstName", "lastName")
            .toList()

        expectThat(result) {
            get(0).and {
                get("firstName").isA<List<String>>().containsExactly("John")
                get("lastName").isA<List<String>>().containsExactly("Doe")
            }
            get(1).and {
                get("firstName").isA<List<String>>().containsExactly("Jane")
                get("lastName").isA<List<String>>().containsExactly("Doe")
            }
            get(2).and {
                get("firstName").isA<List<String>>().containsExactly("Jack")
                get("lastName").isA<List<String>>().containsExactly("Smith")
            }
        }

        assertNoLoadedEdgesOrVerticesInTransactionContext(g)
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun orderByOnMultiValuedPropertyWithIndexWorks() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("fruit").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        this.runOrderByOnMultiValuedPropertyTest(g)
    }


    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun orderByOnMultiValuedPropertyWithoutIndexWorks() {
        // we create no indices here (on purpose)
        this.runOrderByOnMultiValuedPropertyTest(this.graph)
    }

    private fun runOrderByOnMultiValuedPropertyTest(g: ChronoGraph) {
        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "type", "Person",
            "firstName", "John",
            "lastName", "Doe",
            "fruit", mutableListOf("Apple", "Cherry")
        )
        txGraph.addVertex(
            T.id, "p2",
            "type", "Person",
            "firstName", "Jane",
            "lastName", "Doe",
            "fruit", mutableListOf("Banana", "Acerola")
        )
        txGraph.addVertex(
            T.id, "p3",
            "type", "Person",
            "firstName", "Jack",
            "lastName", "Smith",
            "fruit", mutableListOf("Pineapple")
        )
        txGraph.tx().commit()

        val result = g.traversal().V()
            .has("type", "Person")
            .order().by("fruit", Order.asc)
            .valueMap<List<String>>("firstName", "lastName", "fruit")
            .toList()

        expectThat(result) {
            get(0).and {
                get("firstName").isA<List<String>>().containsExactly("Jane")
                get("lastName").isA<List<String>>().containsExactly("Doe")
            }
            get(1).and {
                get("firstName").isA<List<String>>().containsExactly("John")
                get("lastName").isA<List<String>>().containsExactly("Doe")
            }
            get(2).and {
                get("firstName").isA<List<String>>().containsExactly("Jack")
                get("lastName").isA<List<String>>().containsExactly("Smith")
            }
        }
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    fun orderByWorksWithNullValuesWithIndex() {
        val g = this.graph
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("type").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()
        this.runOrderByWorksWithNullValuesTest(g)
    }


    @Test
    fun orderByWorksWithNullValuesWithoutIndex() {
        // we create no indices here (on purpose).
        this.runOrderByWorksWithNullValuesTest(this.graph)
    }

    private fun runOrderByWorksWithNullValuesTest(graph: ChronoGraph) {
        val txGraph = graph.tx().createThreadedTx()
        txGraph.addVertex(
            T.id, "p1",
            "type", "Person",
            "firstName", "John",
        )
        txGraph.addVertex(
            T.id, "p2",
            "type", "Person",
            "firstName", "Jane",
        )
        txGraph.addVertex(
            T.id, "p3",
            "type", "Person",
        )
        txGraph.tx().commit()

        val result = graph.traversal().V()
            .has("type", "Person")
            .order().by("firstName", Order.asc)
            .id()
            .toList()

        expectThat(result) {
            get(0).isEqualTo("p2") // Jane
            get(1).isEqualTo("p1") // John
            get(2).isEqualTo("p3") // <unnamed> (NULL)
        }

        val result2 = graph.traversal().V()
            .has("type", "Person")
            .order().by("firstName", COrder.desc(NullSortPosition.NULLS_FIRST))
            .id()
            .toList()

        expectThat(result2) {
            get(0).isEqualTo("p3") // <unnamed> (NULL)
            get(1).isEqualTo("p1") // John
            get(2).isEqualTo("p2") // Jane
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun valueMapRespectsTransientChanges() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().createThreadedTx().use { tx ->
            tx.addVertex(
                T.id, "p1",
                "firstName", "John",
                "lastName", "Doe"
            )
            tx.addVertex(
                T.id, "p2",
                "firstName", "Jane",
                "lastName", "Doe"
            )
            tx.addVertex(
                T.id, "p3",
                "firstName", "Jack",
                "lastName", "Smith"
            )
            tx.tx().commit()
        }

        g.tx().createThreadedTx().use { tx ->
            val john = tx.vertex("p1")
            john.remove()

            val jane = tx.vertex("p2")
            jane.property("lastName", "Smith")

            tx.addVertex(
                T.id, "p4",
                "firstName", "Sarah",
                "lastName", "Doe"
            )

            // our tx state is now dirty. Run a query to make
            // sure that the query reflects our transient changes.
            val result = tx.traversal().V().valueMap<Any>("firstName", "lastName").with(WithOptions.tokens, WithOptions.ids).toList()

            expectThat(result) {
                // we removed John (p1) transiently, he shouldn't be in the result.
                none { get { this[T.id] }.isEqualTo("p1") }
                one {
                    get { this[T.id] }.isEqualTo("p2")
                    get { this["firstName"] }.isA<List<String>>().containsExactly("Jane")
                    // Jane changed here last name
                    get { this["lastName"] }.isA<List<String>>().containsExactly("Smith")
                }
                one {
                    get { this[T.id] }.isEqualTo("p3")
                    get { this["firstName"] }.isA<List<String>>().containsExactly("Jack")
                    get { this["lastName"] }.isA<List<String>>().containsExactly("Smith")
                }
                one {
                    // Sarah is new in our transient context
                    get { this[T.id] }.isEqualTo("p4")
                    get { this["firstName"] }.isA<List<String>>().containsExactly("Sarah")
                    get { this["lastName"] }.isA<List<String>>().containsExactly("Doe")
                }
            }
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    fun fetchValuesRespectsTransientChanges() {
        val g = this.graph

        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build()
        g.indexManagerOnMaster.reindexAll()

        g.tx().createThreadedTx().use { tx ->
            tx.addVertex(
                T.id, "p1",
                "firstName", "John",
                "lastName", "Doe"
            )
            tx.addVertex(
                T.id, "p2",
                "firstName", "Jane",
                "lastName", "Doe"
            )
            tx.addVertex(
                T.id, "p3",
                "firstName", "Jack",
                "lastName", "Smith"
            )
            tx.tx().commit()
        }

        g.tx().createThreadedTx().use { tx ->
            val john = tx.vertex("p1")
            john.remove()

            val jane = tx.vertex("p2")
            jane.property("lastName", "Travis")

            tx.addVertex(
                T.id, "p4",
                "firstName", "Sarah",
                "lastName", "Baker"
            )

            // our tx state is now dirty. Run a query to make
            // sure that the query reflects our transient changes.
            val result = tx.traversal().V().values<Any>("lastName").toList()

            expectThat(result).containsExactlyInAnyOrder("Travis", "Smith", "Baker")
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    fun canFetchValuesWithoutIndex() {
        val g = this.graph

        val txGraph = g.tx().createThreadedTx()
        txGraph.addVertex(
            "firstName", "John",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            "firstName", "Jane",
            "lastName", "Doe"
        )
        txGraph.addVertex(
            "firstName", "Jack",
            "lastName", "Smith"
        )
        txGraph.tx().commit()

        val names = g.traversal().V().values<String>("firstName").toList()
        expectThat(names).containsExactlyInAnyOrder("John", "Jane", "Jack")
    }


    private fun assertNoLoadedEdgesOrVerticesInTransactionContext(g: ChronoGraph) {
        val ctx = g.tx().currentTransaction.context as GraphTransactionContextInternal
        val loadedVertexIds = ctx.loadedVertexIds.toSet()
        if (loadedVertexIds.isNotEmpty()) {
            fail("This test asserts that no vertices or edges have been loaded, but there have been loaded vertices: ${loadedVertexIds}")
        }
        val loadedEdgeIds = ctx.loadedEdgeIds.toSet()
        if (loadedEdgeIds.isNotEmpty()) {
            fail("This test asserts that no vertices or edges have been loaded, but there have been loaded edges: ${loadedEdgeIds}")
        }
        val modifiedElements = ctx.modifiedElements.toSet()
        if (modifiedElements.isNotEmpty()) {
            fail("This test asserts that no elements have been loaded or modified, but there have been modified elements: ${modifiedElements}")
        }
    }

}