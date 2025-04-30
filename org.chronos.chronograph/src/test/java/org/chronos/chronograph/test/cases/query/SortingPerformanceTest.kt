package org.chronos.chronograph.test.cases.query

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.io.FileUtils
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.has
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronodb.api.TextCompare
import org.chronos.chronograph.api.builder.query.CP
import org.chronos.chronograph.api.builder.query.ordering.COrder
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.common.test.ChronosUnitTest
import org.chronos.common.test.utils.model.person.Person
import org.chronos.common.test.utils.model.person.PersonGenerator
import org.junit.Ignore
import org.junit.Test
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

class SortingPerformanceTest : ChronosUnitTest() {

    companion object{

        private val log = KotlinLogging.logger {}

        private const val PERSON_COUNT = 1_000_000

    }


    @Test
    @Ignore("performance test; run manually")
    fun runTestWithExodus() {
        log.info { "Opening Graph" }
        val graph = ChronoGraph.FACTORY.create()
            .exodusGraph(this.testDirectory)
            .withIdExistenceCheckOnAdd(false)
            .withTransactionAutoStart(false)
            .build()
        try {
            log.info { "Creating indices" }
            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("firstName")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("lastName")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("hobbies")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("pets")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("favoriteColor")
                .acrossAllTimestamps()
                .build()

            log.info { "Reindexing" }
            graph.indexManagerOnMaster.reindexAll()

            log.info { "Generating persons" }
            val persons = PersonGenerator.generateRandomPersons(PERSON_COUNT)
            log.info { "Inserting persons" }
            graph.tx().createThreadedTx().use { tx ->
                var i = 0
                for (person in persons) {
                    tx.addVertex(
                        T.id, person.id,
                        "firstName", person.firstName,
                        "lastName", person.lastName,
                        "hobbies", person.hobbies,
                        "pets", person.pets,
                        "favoriteColor", person.favoriteColor
                    )
                    i++
                    if(i % 10_000 == 0){
                        log.info { "Inserted ${i} persons." }
                    }
                }
                log.info { "Committing ${i} person vertices..." }
                tx.tx().commit()
                log.info { "Commit complete." }
            }

            val dirSize = FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(this.testDirectory))

            log.info { "Graph size on disk: ${dirSize}" }

            log.info { "Waiting for 5 seconds. Attach the profiler NOW!" }
            Thread.sleep(TimeUnit.SECONDS.toMillis(5))

            graph.tx().createThreadedTx().use { tx ->
                log.info { "RUNNING QUERY" }
                val timeBefore = System.currentTimeMillis()
                val sortedValues = tx.traversal().V()
                    .or(
                        has<Vertex>("lastName", CP.containsIgnoreCase("a")),
                        has<Vertex>("lastName", CP.containsIgnoreCase("e"))
                    ).order().by("lastName", Order.asc).by("firstName", COrder.desc(TextCompare.CASE_INSENSITIVE))
                    .valueMap<String>("firstName", "lastName").with(WithOptions.tokens, WithOptions.ids)
                    .toList()
                val timeAfter = System.currentTimeMillis()

                log.info { "Executed Query #1 in ${timeAfter - timeBefore}ms. Fetched ${sortedValues.size} results." }

                println("${"ID".padEnd(40)} | ${"LAST NAME".padEnd(30)} | ${"FIRST NAME".padEnd(30)} | ACTUAL")
                sortedValues.asSequence().take(100).forEach {
                    val vertex = tx.vertex(it[T.id].toString())
                    val fullName = "${vertex.value<String>("lastName")} ${vertex.value<String>("firstName")}"
                    println("${it[T.id].toString().padEnd(40)} | ${it["lastName"].toString().padEnd(30)} | ${it["firstName"].toString().padEnd(30)} | ${fullName}")
                }
            }
        } finally {
            graph.close()
        }

        Thread.sleep(TimeUnit.MINUTES.toMillis(60))
    }

    @Test
    @Ignore("performance test; run manually")
    fun naive(){
        log.info { "Opening Graph" }
        val graph = ChronoGraph.FACTORY.create()
            .exodusGraph(this.testDirectory){
                it.withLruCacheOfSize(100_000)
            }
            .withIdExistenceCheckOnAdd(false)
            .withTransactionAutoStart(false)
            .build()
        try {
            log.info { "Creating indices" }
            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("firstName")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("lastName")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("hobbies")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("pets")
                .acrossAllTimestamps()
                .build()

            graph.indexManagerOnMaster.create()
                .stringIndex()
                .onVertexProperty("favoriteColor")
                .acrossAllTimestamps()
                .build()

            log.info { "Reindexing" }
            graph.indexManagerOnMaster.reindexAll()

            log.info { "Generating persons" }
            val persons = PersonGenerator.generateRandomPersons(PERSON_COUNT)
            log.info { "Inserting persons" }
            graph.tx().createThreadedTx().use { tx ->
                var i = 0
                for (person in persons) {
                    tx.addVertex(
                        T.id, person.id,
                        "firstName", person.firstName,
                        "lastName", person.lastName,
                        "hobbies", person.hobbies,
                        "pets", person.pets,
                        "favoriteColor", person.favoriteColor
                    )
                    i++
                    if(i % 10_000 == 0){
                        log.info { "Inserted ${i} persons." }
                    }
                }
                log.info { "Committing ${i} person vertices..." }
                tx.tx().commit()
                log.info { "Commit complete." }
            }

            val dirSize = FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(this.testDirectory))

            log.info { "Graph size on disk: ${dirSize}" }

            log.info { "Waiting for 5 seconds. Attach the profiler NOW!" }
            Thread.sleep(TimeUnit.SECONDS.toMillis(5))

            graph.tx().createThreadedTx().use { tx ->
                log.info { "RUNNING QUERY" }
                val timeBefore = System.currentTimeMillis()

                val sortedValues = tx.vertices().asSequence()
                    .filter {
                        val lastName = it.value<String>("lastName")
                        lastName.contains("a",ignoreCase = true) || lastName.contains("e", ignoreCase = true)
                    }
                    .sortedWith(
                        Comparator.comparing<Vertex, String> { it.value("lastName") }
                            .thenComparing(Comparator.comparing<Vertex, String> { it.value<String>("firstName").lowercase() }.reversed())
                    )
                    .map {
                        val map = mutableMapOf<Any, Any>()
                        map[T.id] = it.id() as String
                        map["firstName"] = listOf(it.value<String>("firstName"))
                        map["lastName"] = listOf(it.value<String>("lastName"))
                        map
                    }.toList()

                val timeAfter = System.currentTimeMillis()

                log.info { "Executed Query #1 in ${timeAfter - timeBefore}ms. Fetched ${sortedValues.size} results." }

                println("${"ID".padEnd(40)} | ${"LAST NAME".padEnd(30)} | ${"FIRST NAME".padEnd(30)} | ACTUAL")
                sortedValues.asSequence().take(100).forEach {
                    val vertex = tx.vertex(it[T.id].toString())
                    val fullName = "${vertex.value<String>("lastName")} ${vertex.value<String>("firstName")}"
                    println("${it[T.id].toString().padEnd(40)} | ${it["lastName"].toString().padEnd(30)} | ${it["firstName"].toString().padEnd(30)} | ${fullName}")
                }
            }
        } finally {
            graph.close()
        }
    }

    @Test
    @Ignore("performance test; run manually")
    fun speedOfLight(){
        val persons = PersonGenerator.generateRandomPersons(PERSON_COUNT)
        // do a "speed of light" measurement
        val result: List<Map<Any, Any>>
        measureNanoTime {
            result = persons.asSequence()
                .filter { it.lastName.contains("a", ignoreCase = true) || it.lastName.contains("e", ignoreCase = true) }
                .sortedWith(
                    Comparator.comparing<Person?, String?> { it.lastName }
                        .thenComparing(Comparator.comparing<Person, String> { it.firstName.lowercase() }.reversed())
                ).map {
                    val map = mutableMapOf<Any, Any>()
                    map[T.id] = it.id
                    map["firstName"] = listOf(it.firstName)
                    map["lastName"] = listOf(it.lastName)
                    map
                }.toList()
        }.let { println("Most optimal process took ${String.format("%.3f", it.toDouble() / 1000000)}ms") }
        println("Result contains ${result.size} entries.")
    }

}
