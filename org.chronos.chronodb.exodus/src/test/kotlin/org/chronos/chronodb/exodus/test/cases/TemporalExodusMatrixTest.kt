package org.chronos.chronodb.exodus.test.cases

import org.chronos.chronodb.exodus.test.base.EnvironmentTest

class TemporalExodusMatrixTest : EnvironmentTest() {

//    @Test
//    fun canCreateMatrix(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//        matrix shouldBe notNullValue()
//    }
//
//    @Test
//    fun canPutAndGet(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//        val data = mutableMapOf<String, ByteArray>()
//        data.put("Hello", "World".toByteArray())
//        data.put("Foo", "Bar".toByteArray())
//        val insertTime = System.currentTimeMillis()
//        matrix.put(insertTime, data)
//
//        val result1 = matrix.get(System.currentTimeMillis(), "Hello")
//        result1.isHit shouldBe true
//        result1.period shouldBe Period.createOpenEndedRange(insertTime)
//        result1.requestedKey shouldBe QualifiedKey.create("default", "Hello")
//        String(result1.value) shouldBe "World"
//
//        val result2 = matrix.get(System.currentTimeMillis(), "Foo")
//        result2.isHit shouldBe true
//        result2.period shouldBe Period.createOpenEndedRange(insertTime)
//        result2.requestedKey shouldBe QualifiedKey.create("default", "Foo")
//        String(result2.value) shouldBe "Bar"
//    }
//
//    @Test
//    fun canOverride(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Foo"] = "Baz".toByteArray()
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        // check the contents
//        String(matrix.get(insertTime1, "Hello").value) shouldBe "World"
//        String(matrix.get(insertTime1, "Foo").value) shouldBe "Bar"
//        matrix.get(insertTime1, "Number").isHit shouldBe false
//
//        String(matrix.get(insertTime2, "Hello").value) shouldBe "World"
//        String(matrix.get(insertTime2, "Foo").value) shouldBe "Baz"
//        String(matrix.get(insertTime2, "Number").value) shouldBe "42"
//
//        val history = Lists.newArrayList(matrix.history(System.currentTimeMillis(), "Foo"))
//        history[0] shouldBe insertTime2
//        history[1] shouldBe insertTime1
//    }
//
//    @Test
//    fun canDelete(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        val result1 = matrix.get(insertTime2, "Hello")
//        result1.isHit shouldBe true
//        result1.value shouldBe nullValue()
//    }
//
//    @Test
//    fun canCreateAndEditMultipleMatrices(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        val result1 = matrix.get(insertTime2, "Hello")
//        result1.isHit shouldBe true
//        result1.value shouldBe nullValue()
//
//        val matrix2 = TemporalExodusMatrix(environment, "test2", "default", System.currentTimeMillis())
//
//        sleep(1)
//
//        // insert #1
//        val data3 = mutableMapOf<String, ByteArray>()
//        data3["abc"] = "123".toByteArray()
//        data3["doremi"] = "abc".toByteArray()
//        val insertTime3 = System.currentTimeMillis()
//        matrix2.put(insertTime3, data3)
//
//        val result3 = matrix2.get(insertTime3, "abc")
//        result3.isHit shouldBe true
//        String(result3.value) shouldBe "123"
//
//        val result4 = matrix2.get(insertTime3, "Hello")
//        result4.isHit shouldBe false
//
//        val result5 = matrix.get(insertTime2, "Hello")
//        result5.isHit shouldBe true
//        result5.value shouldBe nullValue()
//
//    }
//
//    @Test
//    fun canPurgeEntry(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        // purge the deletion
//        matrix.purgeEntry("Hello", insertTime2)
//
//        // the value should exist (again)
//        val result = matrix.get(System.currentTimeMillis(), "Hello")
//        result.isHit shouldBe true
//        result.period shouldBe Period.createOpenEndedRange(insertTime1)
//        String(result.value) shouldBe "World"
//
//        val history = Lists.newArrayList(matrix.history(System.currentTimeMillis(), "Hello"))
//        history.size shouldBe 1
//        history[0] shouldBe insertTime1
//    }
//
//    @Test
//    fun canCalculateKeysetModifications(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        val mod1 = matrix.keySetModifications(System.currentTimeMillis())
//        mod1.additions shouldBe setOf("Foo", "Number")
//        mod1.removals shouldBe setOf("Hello")
//
//        val mod2 = matrix.keySetModifications(insertTime1)
//        mod2.additions shouldBe setOf("Hello", "Foo")
//        mod2.removals shouldBe setOf<String>()
//    }
//
//    @Test
//    fun canCalculateModificationsBetween(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        val modificationsBetween = Lists.newArrayList(matrix.getModificationsBetween(0, System.currentTimeMillis()))
//        modificationsBetween.size shouldBe 4
//
//        val keys = modificationsBetween.asSequence().map { it.key }.distinct().toList()
//        keys.size shouldBe 3
//        keys.contains("Hello") shouldBe true
//        keys.contains("Foo") shouldBe true
//        keys.contains("Number") shouldBe true
//
//        val modBetween2 = matrix.getModificationsBetween(insertTime2, System.currentTimeMillis()).asSequence().toList()
//        modBetween2.size shouldBe 2
//
//        val keys2 = modBetween2.asSequence().map { it.key }.distinct().toList()
//        keys2.size shouldBe 2
//        keys2.contains("Hello") shouldBe true
//        keys2.contains("Number") shouldBe true
//    }
//
//    @Test
//    fun canPerformRollback(){
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        // insert #1
//        val data = mutableMapOf<String, ByteArray>()
//        data["Hello"] = "World".toByteArray()
//        data["Foo"] = "Bar".toByteArray()
//        val insertTime1 = System.currentTimeMillis()
//        matrix.put(insertTime1, data)
//
//        sleep(1)
//
//        // insert #2
//        val data2 = mutableMapOf<String, ByteArray>()
//        data2["Hello"] = ByteArray(0)
//        data2["Number"] = "42".toByteArray()
//        val insertTime2 = System.currentTimeMillis()
//        matrix.put(insertTime2, data2)
//
//        val getDelete = matrix.get(System.currentTimeMillis(), "Hello")
//        getDelete.isHit shouldBe true
//        getDelete.period shouldBe Period.createOpenEndedRange(insertTime2)
//        getDelete.value shouldBe nullValue()
//
//        matrix.rollback(insertTime1)
//        val get = matrix.get(System.currentTimeMillis(), "Hello")
//        get.isHit shouldBe true
//        get.period shouldBe Period.createOpenEndedRange(insertTime1)
//        String(get.value) shouldBe "World"
//    }
}