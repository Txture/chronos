package org.chronos.chronograph.test.cases.query.strategies

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronodb.test.base.InstantiateChronosWith
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP
import org.chronos.chronograph.internal.impl.optimizer.step.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__` as AnonymousTraversal
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.jupiter.api.Assertions.fail
import strikt.api.expectThat
import strikt.assertions.*

@Category(IntegrationTest::class)
class FetchValuesFromSecondaryIndexStrategyTest : AllChronoGraphBackendsTest() {

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun propertiesStepIsReplacedOnlyOnce() {
        val graph = this.graph

        val traversal = graph.traversal().inject(0).union(
            AnonymousTraversal.V<Vertex>()
                .has("kind", "entity")
                .has("type", P.within("a", "b", "c"))
                .has("myprop") // has("myprop") is translated as filter(values("myprop"))
                .fold()
                .map { it.get().size }
        )

        // trigger the strategies by calling "hasNext()" on it
        traversal.hasNext()

        val propertiesSteps = TraversalHelper.getStepsOfAssignableClassRecursively(PropertiesStep::class.java, traversal.asAdmin())
        expectThat(propertiesSteps).single().isA<ChronoGraphPropertiesStep<*>>()

        val propertiesStep = propertiesSteps.single()

        val barriers = TraversalHelper.getStepsOfAssignableClassRecursively(ChronoGraphPrefetchingBarrierStep::class.java, traversal.asAdmin())
        expectThat(barriers).isNotEmpty().all {
            get { getTransitiveClients(this) }.single().isA<ChronoGraphPropertiesStep<*>>().isSameInstanceAs(propertiesStep)
        }
    }

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun canEvaluateLargePropertiesRequestWithoutIndex(){
        val graph = this.graph

        val idsOfVerticesWithProperty = mutableSetOf<String>()
        graph.tx().createThreadedTx().use { txGraph ->
            repeat(Prefetcher.DEFAULT_PREFETCH_INDEX_QUERY_MIN_ELEMENTS + 10) { i ->
                val vertexId = "v${i.toString().padStart(8, '0')}"
                val vertex = txGraph.addVertex(T.id, vertexId)
                vertex.property("kind", "entity")
                if(i % 2 == 0){
                    vertex.property("foo", "bar")
                    idsOfVerticesWithProperty += vertexId
                }
            }
            txGraph.tx().commit()
        }

        graph.tx().createThreadedTx().use { txGraph ->
            val traversal = txGraph.traversal().inject(0).union(
                AnonymousTraversal.V<Vertex>()
                    .has("kind", "entity")
                    .has("foo") // has("foo") is translated as filter(values("foo"))
                    .id()
                    .fold()
                    .map { it.get() as List<String> }
            )

            val vertices = traversal.next()

            expectThat(vertices.toSet()).containsExactlyInAnyOrder(idsOfVerticesWithProperty)
        }
    }

    @Test
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUES_STEP, value = "true")
    @InstantiateChronosWith(property = USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, value = "true")
    fun canEvaluateLargePropertiesRequestWithIndex(){
        val graph = this.graph

        val indexManager = graph.indexManagerOnMaster
        indexManager.create().stringIndex().onVertexProperty("kind").acrossAllTimestamps().build()
        indexManager.create().stringIndex().onVertexProperty("foo").acrossAllTimestamps().build()
        indexManager.reindexAll()

        val idsOfVerticesWithProperty = mutableSetOf<String>()
        graph.tx().createThreadedTx().use { txGraph ->
            repeat(Prefetcher.DEFAULT_PREFETCH_INDEX_QUERY_MIN_ELEMENTS + 10) { i ->
                val vertexId = "v${i.toString().padStart(8, '0')}"
                val vertex = txGraph.addVertex(T.id, vertexId)
                vertex.property("kind", "entity")
                if(i % 2 == 0){
                    vertex.property("foo", "bar")
                    idsOfVerticesWithProperty += vertexId
                }
            }
            txGraph.tx().commit()
        }

        graph.tx().createThreadedTx().use { txGraph ->
            val traversal = txGraph.traversal().inject(0).union(
                AnonymousTraversal.V<Vertex>()
                    .has("kind", "entity")
                    .has("foo") // has("foo") is translated as filter(values("foo"))
                    .id()
                    .fold()
                    .map { it.get() as List<String> }
            )

            val vertices = traversal.next()

            expectThat(vertices.toSet()).containsExactlyInAnyOrder(idsOfVerticesWithProperty)
        }
    }


    private fun getTransitiveClients(source: Prefetching): Set<Prefetching> {
        return when(source){
            is ChronoGraphPrefetchingBarrierStep<*> -> source.clients.asSequence().flatMap { getTransitiveClients(it) }.toSet()
            is ChronoGraphPropertiesStep<*> -> setOf(source)
            is ChronoGraphPropertyMapStep<*, *> -> setOf(source)
            is Prefetcher -> setOf(source)
            else -> fail("Unknown subtype of Prefetching: ${source::class.java.name}")
        }
    }

}