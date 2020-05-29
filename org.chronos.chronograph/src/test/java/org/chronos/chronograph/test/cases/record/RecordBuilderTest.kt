package org.chronos.chronograph.test.cases.record

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions.list
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.T
import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.inmemory.InMemoryChronoDB
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.api.structure.ChronoVertex
import org.chronos.chronograph.api.structure.record.IEdgeRecord
import org.chronos.chronograph.api.structure.record.IVertexRecord
import org.chronos.common.test.junit.categories.UnitTest
import org.chronos.common.test.utils.model.person.Person
import org.hamcrest.Matchers.`is`
import org.junit.Assert.*
import org.junit.Test
import org.junit.experimental.categories.Category
import java.util.*

@Category(UnitTest::class)
class RecordBuilderTest {

    @Test
    fun canCreateIEdgeRecordFromScratch() {
        val record = IEdgeRecord.builder().withId("1234")
            .withLabel("knows")
            .withInVertex("John")
            .withOutVertex("Jane")
            .withProperty("foo", "bar")
            .withProperty("hello", "world")
            .withProperty("foo", "baz")
            .build()
        assertNotNull(record)
        assertThat(record.id, `is`("1234"))
        assertThat(record.label, `is`("knows"))
        assertThat(record.inVertexId, `is`("John"))
        assertThat(record.outVertexId, `is`("Jane"))
        assertThat(record.properties.size, `is`(2))
        assertEquals("baz", record.properties.asSequence().filter { it.key == "foo" }.single().value)
        assertEquals("world", record.properties.asSequence().filter { it.key == "hello" }.single().value)
    }

    @Test
    fun canCreateIEdgeRecordFromAnotherRecord() {
        val record = IEdgeRecord.builder().withId("1234")
            .withLabel("knows")
            .withOutVertex("John")
            .withInVertex("Jane")
            .withProperty("foo", "bar")
            .withProperty("hello", "world")
            .withProperty("foo", "baz")
            .build()

        val record2 = IEdgeRecord.builder().fromRecord(record)
            .withProperty("hello", "ChronoGraph")
            .withoutProperty("foo")
            .build()
        assertThat(record2.id, `is`("1234"))
        assertThat(record2.label, `is`("knows"))
        assertThat(record2.inVertexId, `is`("Jane"))
        assertThat(record2.outVertexId, `is`("John"))
        assertThat(record2.properties.size, `is`(1))
        assertEquals("ChronoGraph", record2.getProperty("hello").value)
    }

    @Test
    fun canCreateIEdgeRecordFromRealEdge() {
        ChronoGraph.FACTORY.create().graphOnChronoDB(ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER)).build().use { g ->
            val vJohn = g.addVertex(T.id, "John")
            val vJane = g.addVertex(T.id, "Jane")
            val edge = vJohn.addEdge("knows", vJane, T.id, "1234", "foo", "bar", "hello", "world")

            val record = IEdgeRecord.builder().fromEdge(edge).build()
            assertThat(record.id, `is`("1234"))
            assertThat(record.label, `is`("knows"))
            assertThat(record.outVertexId, `is`("John"))
            assertThat(record.inVertexId, `is`("Jane"))
            assertThat(record.properties.size, `is`(2))
            assertEquals("bar", record.properties.asSequence().filter { it.key == "foo" }.single().value)
            assertEquals("world", record.properties.asSequence().filter { it.key == "hello" }.single().value)
        }
    }

    @Test
    fun canCreateIVertexRecordFromScratch() {
        val record = IVertexRecord.builder()
            .withId("v1")
            .withLabel("person")
            .withProperty("firstName", "John")
            .withProperty("lastName", "Doe")
            .withMetaProperty("lastName", "was", "Smith")
            .withEdge(Direction.OUT, "knows", "e1", "v2")
            .withEdge(Direction.IN, "knows", "e2", "v3")
            .withEdge(Direction.OUT, "self", "e100", "v1")
            .withEdge(Direction.IN, "self", "e100", "v1")
            .build()

        assertEquals("v1", record.id)
        assertEquals("person", record.label)
        assertEquals("John", record.getProperty("firstName").value)
        assertEquals("Doe", record.getProperty("lastName").value)
        assertEquals("Smith", record.getProperty("lastName").properties.asSequence().filter { it.key == "was" }.single().value.value)
        assertEquals("e1", record.outgoingEdges.asSequence().filter { it.label == "knows" }.single().record.edgeId)
        assertEquals("e2", record.incomingEdges.asSequence().filter { it.label == "knows" }.single().record.edgeId)
        assertEquals("v2", record.outgoingEdges.asSequence().filter { it.label == "knows" }.single().record.otherEndVertexId)
        assertEquals("v3", record.incomingEdges.asSequence().filter { it.label == "knows" }.single().record.otherEndVertexId)
        assertEquals("v1", record.outgoingEdges.asSequence().filter { it.label == "self" }.single().record.otherEndVertexId)
        assertEquals("v1", record.incomingEdges.asSequence().filter { it.label == "self" }.single().record.otherEndVertexId)
        assertEquals("knows", record.outgoingEdges.asSequence().filter { it.label == "knows" }.single().label)
        assertEquals("knows", record.incomingEdges.asSequence().filter { it.label == "knows" }.single().label)
    }

    @Test
    fun canCreateIVertexRecordFromAnotherRecord() {
        val record = IVertexRecord.builder()
            .withId("v1")
            .withLabel("person")
            .withProperty("firstName", "John")
            .withProperty("lastName", "Doe")
            .withMetaProperty("lastName", "was", "Smith")
            .withEdge(Direction.OUT, "knows", "e1", "v2")
            .withEdge(Direction.IN, "knows", "e2", "v3")
            .withEdge(Direction.OUT, "self", "e100", "v1")
            .withEdge(Direction.IN, "self", "e100", "v1")
            .build()

        val record2 = IVertexRecord.builder().fromRecord(record)
            .withProperty("lastName", "Jackson")
            .withEdge(Direction.OUT, "marriedTo", "e3", "v4")
            .build()

        assertEquals(0, record2.getProperty("lastName").properties.size)
        assertEquals("Jackson", record2.getProperty("lastName").value)
        assertEquals("v4", record2.getOutgoingEdges("marriedTo").single().record.otherEndVertexId)
    }

    @Test
    fun canCreateIVertexRecordFromRealVertex() {
        ChronoGraph.FACTORY.create().graphOnChronoDB(ChronoDB.FACTORY.create().database(InMemoryChronoDB.BUILDER)).build().use { g ->
            val vJohn = g.addVertex(T.id, "vJohn", "firstName", "John", "lastName", "Doe")
            val vJane = g.addVertex(T.id, "vJane")
            vJohn.addEdge("knows", vJane, T.id, "1234", "foo", "bar", "hello", "world")

            vJohn.property<String>("lastName").property("was", "Smith")

            val record = IVertexRecord.builder().fromVertex(vJohn).build()
            assertEquals("vJohn", record.id)
            assertEquals(ChronoVertex.DEFAULT_LABEL, record.label)
            assertEquals("John", record.getProperty("firstName").value)
            assertEquals("Doe", record.getProperty("lastName").value)
            assertEquals("Smith", record.getProperty("lastName").properties.asSequence().filter { it.value.key == "was" }.single().value.value)
            assertEquals("vJane", record.getOutgoingEdges("knows").single().record.otherEndVertexId)
        }
    }

    @Test
    fun copyingVertexRecordNeverYieldsUnmodifiableLists() {
        val record = IVertexRecord.builder()
            .withId("v1")
            .withLabel("vertex")
            .withProperty("test1", Lists.newArrayList("hello", "world"))
            .withProperty("test2", Lists.newArrayList(true, false))
            .withProperty("test3", Lists.newArrayList(1.toByte(), 2.toByte()))
            .withProperty("test4", Lists.newArrayList(1.toShort(), 2.toShort()))
            .withProperty("test5", Lists.newArrayList('1', '2'))
            .withProperty("test6", Lists.newArrayList(1, 2))
            .withProperty("test7", Lists.newArrayList(1L, 2L))
            .withProperty("test8", Lists.newArrayList(1.0f, 2.0f))
            .withProperty("test9", Lists.newArrayList(1.0, 2.0))
            .withProperty("test10", Lists.newArrayList(Date()))
            .build()

        for (property in record.properties) {
            assertIsUnmodifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }

        val record2 = IVertexRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsUnmodifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }
    }

    @Test
    fun copyingVertexRecordNeverYieldsUnmodifiableListsWithCustomObjects() {
        val record = IVertexRecord.builder()
            .withId("v1")
            .withLabel("vertex")
            .withProperty("test1", Lists.newArrayList(Person("John", "Doe")))
            .build()

        assertIsModifiableList(record.getProperty("test1").value as List<*>)
        assertIsModifiableList(record.getProperty("test1").serializationSafeValue as List<*>)
        // we should receive list copies here
        assertNotSame(record.getProperty("test1").value, record.getProperty("test1").value)

        val record2 = IVertexRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsModifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }
    }

    @Test
    fun copyingEdgeRecordNeverYieldsUnmodifiableLists() {
        val record = IEdgeRecord.builder()
            .withId("e1")
            .withInVertex("v1")
            .withOutVertex("v2")
            .withLabel("test")
            .withProperty("test1", Lists.newArrayList("hello", "world"))
            .withProperty("test2", Lists.newArrayList(true, false))
            .withProperty("test3", Lists.newArrayList(1.toByte(), 2.toByte()))
            .withProperty("test4", Lists.newArrayList(1.toShort(), 2.toShort()))
            .withProperty("test5", Lists.newArrayList('1', '2'))
            .withProperty("test6", Lists.newArrayList(1, 2))
            .withProperty("test7", Lists.newArrayList(1L, 2L))
            .withProperty("test8", Lists.newArrayList(1.0f, 2.0f))
            .withProperty("test9", Lists.newArrayList(1.0, 2.0))
            .withProperty("test10", Lists.newArrayList(Date()))
            .build()

        for (property in record.properties) {
            assertIsUnmodifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }

        val record2 = IEdgeRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsUnmodifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }
    }


    @Test
    fun copyingEdgeRecordNeverYieldsUnmodifiableListsWithCustomObjects() {
        val record = IEdgeRecord.builder()
            .withId("e1")
            .withInVertex("v1")
            .withOutVertex("v2")
            .withLabel("test")
            .withProperty("test1", Lists.newArrayList(Person("John", "Doe")))
            .build()

        assertIsModifiableList(record.getProperty("test1").value as List<*>)
        assertIsModifiableList(record.getProperty("test1").serializationSafeValue as List<*>)
        // we should receive list copies here
        assertNotSame(record.getProperty("test1").value, record.getProperty("test1").value)

        val record2 = IEdgeRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsModifiableList(property.value as List<*>)
            assertIsModifiableList(property.serializationSafeValue as List<*>)
        }
    }


    @Test
    fun copyingVertexRecordNeverYieldsUnmodifiableSets() {
        val record = IVertexRecord.builder()
            .withId("v1")
            .withLabel("vertex")
            .withProperty("test1", Sets.newHashSet("hello", "world"))
            .withProperty("test2", Sets.newHashSet(true, false))
            .withProperty("test3", Sets.newHashSet(1.toByte(), 2.toByte()))
            .withProperty("test4", Sets.newHashSet(1.toShort(), 2.toShort()))
            .withProperty("test5", Sets.newHashSet('1', '2'))
            .withProperty("test6", Sets.newHashSet(1, 2))
            .withProperty("test7", Sets.newHashSet(1L, 2L))
            .withProperty("test8", Sets.newHashSet(1.0f, 2.0f))
            .withProperty("test9", Sets.newHashSet(1.0, 2.0))
            .withProperty("test10", Sets.newHashSet(Date()))
            .build()

        for (property in record.properties) {
            assertIsUnmodifiableSet(property.value as Set<*>)
            assertIsModifiableSet(property.serializationSafeValue as Set<*>)
        }

        val record2 = IVertexRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsUnmodifiableSet(property.value as Set<*>)
            assertIsModifiableSet(property.serializationSafeValue as Set<*>)
        }
    }

    @Test
    fun copyingEdgeRecordNeverYieldsUnmodifiableSets() {
        val record = IEdgeRecord.builder()
            .withId("e1")
            .withInVertex("v1")
            .withOutVertex("v2")
            .withLabel("test")
            .withProperty("test1", Sets.newHashSet("hello", "world"))
            .withProperty("test2", Sets.newHashSet(true, false))
            .withProperty("test3", Sets.newHashSet(1.toByte(), 2.toByte()))
            .withProperty("test4", Sets.newHashSet(1.toShort(), 2.toShort()))
            .withProperty("test5", Sets.newHashSet('1', '2'))
            .withProperty("test6", Sets.newHashSet(1, 2))
            .withProperty("test7", Sets.newHashSet(1L, 2L))
            .withProperty("test8", Sets.newHashSet(1.0f, 2.0f))
            .withProperty("test9", Sets.newHashSet(1.0, 2.0))
            .withProperty("test10", Sets.newHashSet(Date()))
            .build()

        for (property in record.properties) {
            assertIsUnmodifiableSet(property.value as Set<*>)
            assertIsModifiableSet(property.serializationSafeValue as Set<*>)
        }

        val record2 = IEdgeRecord.builder().fromRecord(record).build()

        assertNotSame(record, record2)

        for (property in record2.properties) {
            assertIsUnmodifiableSet(property.value as Set<*>)
            assertIsModifiableSet(property.serializationSafeValue as Set<*>)
        }
    }

    private fun assertIsUnmodifiableList(list: List<*>) {
        assertEquals(Collections.unmodifiableList(mutableListOf("")).javaClass, list.javaClass)
    }

    private fun assertIsModifiableList(list: List<*>) {
        assertNotEquals(Collections.unmodifiableList(mutableListOf("")).javaClass, list.javaClass)
    }

    private fun assertIsUnmodifiableSet(set: Set<*>) {
        assertEquals(Collections.unmodifiableSet(mutableSetOf("")).javaClass, set.javaClass)
    }

    private fun assertIsModifiableSet(set: Set<*>) {
        assertNotEquals(Collections.unmodifiableSet(mutableSetOf("")).javaClass, set.javaClass)
    }
}