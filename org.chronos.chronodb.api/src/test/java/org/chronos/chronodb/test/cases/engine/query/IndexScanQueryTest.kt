package org.chronos.chronodb.test.cases.engine.query

import org.chronos.chronodb.api.NullSortPosition.NULLS_LAST
import org.chronos.chronodb.api.Order.ASCENDING
import org.chronos.chronodb.api.Order.DESCENDING
import org.chronos.chronodb.api.Sort
import org.chronos.chronodb.api.TextCompare.CASE_INSENSITIVE
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer
import org.chronos.chronodb.test.cases.util.model.person.LastNameIndexer
import org.chronos.chronodb.test.cases.util.model.person.PetsIndexer
import org.chronos.common.test.utils.model.person.Person
import org.junit.Assert.assertEquals
import org.junit.Test

class IndexScanQueryTest : AllChronoDBBackendsTest() {

    @Test
    fun canGetIndexValuesOnMaster() {
        runTestCanGetIndexValuesOnMaster(false)
    }

    @Test
    fun canGetIndexValuesOnMasterWithDirtyIndex() {
        runTestCanGetIndexValuesOnMaster(true)
    }

    private fun runTestCanGetIndexValuesOnMaster(dirtyIndex: Boolean) {
        val db = this.chronoDB
        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        if (!dirtyIndex) {
            indexManager.reindexAll()
        }

        val tx1 = db.tx()
        tx1.put("p1", Person("John", "Doe").also { it.pets = setOf("Cat", "Dog") })
        tx1.put("p2", Person("John", "Smith").also { it.pets = setOf("Cat", "Fish") })
        tx1.put("p3", Person("John", "Walker"))
        val commit1 = tx1.commit()

        val tx2 = db.tx()
        tx2.put("p4", Person("Jane", "Doe"))
        tx2.put("p5", Person("Jack", "Smith").also { it.pets = setOf("Cat") })
        tx2.put("p2", Person("John", "Smith").also { it.pets = setOf("Cat", "Snake") })
        tx2.remove("p1")
        val commit2 = tx2.commit()

        val master = db.branchManager.masterBranch

        // =====================================================================================================
        // Check "getIndexedValues" for firstName

        assertEquals(
            mapOf(
                "John" to setOf("p1", "p2", "p3"),
            ),
            indexManager.getIndexedValues(commit1, master, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p1", "p3"),
            ),
            indexManager.getIndexedValues(commit1, master, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p2", "p3"),
                "Jane" to setOf("p4"),
                "Jack" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, master, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p3"),
                "Jack" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, master, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        // =====================================================================================================
        // Check "getIndexedValuesByKey" for firstName

        assertEquals(
            mapOf(
                "p1" to setOf("John"),
                "p2" to setOf("John"),
                "p3" to setOf("John")
            ),
            indexManager.getIndexedValuesByKey(commit1, master, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "p1" to setOf("John"),
                "p3" to setOf("John")
            ),
            indexManager.getIndexedValuesByKey(commit1, master, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "p2" to setOf("John"),
                "p3" to setOf("John"),
                "p4" to setOf("Jane"),
                "p5" to setOf("Jack")
            ),
            indexManager.getIndexedValuesByKey(commit2, master, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "p3" to setOf("John"),
                "p5" to setOf("Jack")
            ),
            indexManager.getIndexedValuesByKey(commit2, master, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        // =====================================================================================================
        // Check "getIndexedValues" for pets

        assertEquals(
            mapOf(
                "Cat" to setOf("p1", "p2"),
                "Dog" to setOf("p1"),
                "Fish" to setOf("p2")
            ),
            indexManager.getIndexedValues(commit1, master, "default", "pets"),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p1"),
                "Dog" to setOf("p1")
            ),
            indexManager.getIndexedValues(commit1, master, "default", "pets", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p2", "p5"),
                "Snake" to setOf("p2")
            ),
            indexManager.getIndexedValues(commit2, master, "default", "pets"),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, master, "default", "pets", setOf("p1", "p3", "p5")),
        )
    }

    @Test
    fun canGetIndexValuesOnMasterWithBigDataset() {
        val db = this.chronoDB
        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()

        indexManager.reindexAll()

        val tx1 = db.tx()
        for (i in 0..10000) {
            tx1.put("p${i}", Person("P${i}", "Testificate"))
        }
        val commit1 = tx1.commit()


        val tx2 = db.tx()
        for (i in 0..10000 step 2) {
            tx2.remove("p${i}")
        }
        val commit2 = tx2.commit()


        val master = db.branchManager.masterBranch

        val expectedMap1 = (0..10000).asSequence().map { "P${it}" }.map { it to setOf(it.lowercase()) }.toMap()
        val actualMap1 = indexManager.getIndexedValues(commit1, master, "default", "firstName")

        for ((indexValue, expectedKeys) in expectedMap1) {
            val actualKeys = actualMap1[indexValue] ?: emptySet()
            assertEquals(expectedKeys, actualKeys)
        }

        val expectedMap2 = expectedMap1.asSequence()
            .filter { it.key.removePrefix("P").toInt() % 2 != 0 }
            .map { it.key to it.value }
            .toMap()
        val actualMap2 = indexManager.getIndexedValues(commit2, master, "default", "firstName")

        for ((indexValue, expectedKeys) in expectedMap2) {
            val actualKeys = actualMap2[indexValue] ?: emptySet()
            assertEquals(expectedKeys, actualKeys)
        }
    }

    @Test
    fun canGetIndexValuesOnBranchWithBigDataset() {
        val db = this.chronoDB
        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.reindexAll()

        val tx1 = db.tx()
        for (i in 0..10000) {
            tx1.put("p${i}", Person("P${i}", "Testificate"))
        }
        val commit1 = tx1.commit()

        val myBranch = db.branchManager.createBranch("myBranch")

        val tx2 = db.tx(myBranch.name)
        for (i in 0..10000 step 2) {
            tx2.remove("p${i}")
        }
        val commit2 = tx2.commit()

        val expectedMap1 = (0..10000).asSequence().map { "P${it}" }.map { it to setOf(it.lowercase()) }.toMap()
        val actualMap1 = indexManager.getIndexedValues(commit1, myBranch, "default", "firstName")

        for ((indexValue, expectedKeys) in expectedMap1) {
            val actualKeys = actualMap1[indexValue] ?: emptySet()
            assertEquals(expectedKeys, actualKeys)
        }

        val expectedMap2 = expectedMap1.asSequence()
            .filter { it.key.removePrefix("P").toInt() % 2 != 0 }
            .map { it.key to it.value }
            .toMap()
        val actualMap2 = indexManager.getIndexedValues(commit2, myBranch, "default", "firstName")

        for ((indexValue, expectedKeys) in expectedMap2) {
            val actualKeys = actualMap2[indexValue] ?: emptySet()
            assertEquals(expectedKeys, actualKeys)
        }
    }

    @Test
    fun canGetIndexValuesOnBranch() {
        runTestCanGetIndexValuesOnBranch(false)
    }

    @Test
    fun canGetIndexValuesOnBranchWithDirtyIndex() {
        runTestCanGetIndexValuesOnBranch(true)
    }

    private fun runTestCanGetIndexValuesOnBranch(dirtyIndex: Boolean) {
        val db = this.chronoDB

        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        if (!dirtyIndex) {
            indexManager.reindexAll()
        }

        val tx1 = db.tx()
        tx1.put("p1", Person("John", "Doe").also { it.pets = setOf("Cat", "Dog") })
        tx1.put("p2", Person("John", "Smith").also { it.pets = setOf("Cat", "Fish") })
        tx1.put("p3", Person("John", "Walker"))
        val commit1 = tx1.commit()

        val myBranch = db.branchManager.createBranch("myBranch")

        val tx2 = db.tx(myBranch.name)
        tx2.put("p4", Person("Jane", "Doe"))
        tx2.put("p5", Person("Jack", "Smith").also { it.pets = setOf("Cat") })
        tx2.put("p2", Person("John", "Smith").also { it.pets = setOf("Cat", "Snake") })
        tx2.remove("p1")
        val commit2 = tx2.commit()


        // =====================================================================================================
        // Check "getIndexedValues" for firstName

        assertEquals(
            mapOf(
                "John" to setOf("p1", "p2", "p3"),
            ),
            indexManager.getIndexedValues(commit1, myBranch, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p1", "p3"),
            ),
            indexManager.getIndexedValues(commit1, myBranch, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p2", "p3"),
                "Jane" to setOf("p4"),
                "Jack" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, myBranch, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "John" to setOf("p3"),
                "Jack" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, myBranch, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        // =====================================================================================================
        // Check "getIndexedValuesByKey" for firstName

        assertEquals(
            mapOf(
                "p1" to setOf("John"),
                "p2" to setOf("John"),
                "p3" to setOf("John")
            ),
            indexManager.getIndexedValuesByKey(commit1, myBranch, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "p1" to setOf("John"),
                "p3" to setOf("John")
            ),
            indexManager.getIndexedValuesByKey(commit1, myBranch, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "p2" to setOf("John"),
                "p3" to setOf("John"),
                "p4" to setOf("Jane"),
                "p5" to setOf("Jack")
            ),
            indexManager.getIndexedValuesByKey(commit2, myBranch, "default", "firstName"),
        )

        assertEquals(
            mapOf(
                "p3" to setOf("John"),
                "p5" to setOf("Jack")
            ),
            indexManager.getIndexedValuesByKey(commit2, myBranch, "default", "firstName", setOf("p1", "p3", "p5")),
        )

        // =====================================================================================================
        // Check "getIndexedValues" for pets

        assertEquals(
            mapOf(
                "Cat" to setOf("p1", "p2"),
                "Dog" to setOf("p1"),
                "Fish" to setOf("p2")
            ),
            indexManager.getIndexedValues(commit1, myBranch, "default", "pets"),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p1"),
                "Dog" to setOf("p1")
            ),
            indexManager.getIndexedValues(commit1, myBranch, "default", "pets", setOf("p1", "p3", "p5")),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p2", "p5"),
                "Snake" to setOf("p2")
            ),
            indexManager.getIndexedValues(commit2, myBranch, "default", "pets"),
        )

        assertEquals(
            mapOf(
                "Cat" to setOf("p5")
            ),
            indexManager.getIndexedValues(commit2, myBranch, "default", "pets", setOf("p1", "p3", "p5")),
        )
    }

    @Test
    fun canSortEntriesOnMaster() {
        val db = this.chronoDB
        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.reindexAll()

        val tx1 = db.tx()
        tx1.put("p1", Person("John", "Doe").also { it.pets = setOf("Cat", "Dog") })
        tx1.put("p2", Person("john", "Smith").also { it.pets = setOf("cat", "Fish") })
        tx1.put("p3", Person("John", "Walker"))
        tx1.commit()

        val tx2 = db.tx()
        tx2.put("p4", Person("Jane", "Doe"))
        tx2.put("p5", Person("Jack", "Smith").also { it.pets = setOf("cat") })
        tx2.put("p2", Person("john", "Smith").also { it.pets = setOf("Cat", "Snake") })
        tx2.put("p6", Person("John", "Jackson").also { it.pets = setOf("Zebra") })
        tx2.remove("p1")
        val commit2 = tx2.commit()

        val master = db.branchManager.masterBranch

        val sorted = indexManager.sortKeysWithIndex(
            commit2,
            master,
            "default",
            setOf("p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"),
            Sort.by("firstName", ASCENDING, CASE_INSENSITIVE, NULLS_LAST).thenBy("pets", DESCENDING, CASE_INSENSITIVE, NULLS_LAST)
        )
        assertEquals(
            listOf("p5", "p4", "p6", "p2", "p3", "p1", "p7", "p8"),
            sorted
        )
    }

    @Test
    fun canSortBigDatasetOnMaster() {
        val db = this.chronoDB
        val indexManager = db.indexManager
        indexManager.createIndex()
            .withName("firstName")
            .withIndexer(FirstNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()

        indexManager.createIndex()
            .withName("lastName")
            .withIndexer(LastNameIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()

        indexManager.createIndex()
            .withName("pets")
            .withIndexer(PetsIndexer())
            .onMaster()
            .acrossAllTimestamps()
            .build()
        indexManager.reindexAll()

        val tx1 = db.tx()
        for (i in 0..10000) {
            tx1.put("p${i}", Person("P${i}", "Testificate"))
        }
        val commit1 = tx1.commit()
        val master = db.branchManager.masterBranch

        val expected = (0..10000).asSequence().map { "P${it}" }.sorted().toList()
        val actual = indexManager.sortKeysWithIndex(
            commit1, master, "default", expected.asSequence().map { it.lowercase() }.toSet(), Sort.by("firstName", ASCENDING)
        )
        assertEquals(expected.size, actual.size)
        for (i in expected.indices) {
            val expectedElement = expected[i].lowercase()
            val actualElement = actual[i]
            assertEquals("Ordering failure at index ${i}", expectedElement, actualElement)
        }
    }

}