package org.chronos.chronodb.test.cases.cache.mosaic

import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.internal.api.GetResult
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.cache.ChronoDBCache
import org.chronos.chronodb.internal.impl.cache.headfirst.HeadFirstCache
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache
import org.chronos.common.test.ChronosUnitTest
import org.chronos.common.test.junit.categories.UnitTest
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.jupiter.api.Disabled
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime

@Category(UnitTest::class)
class HeadFirstCacheTest : ChronosUnitTest() {

    @Test
    fun canCreateHeadFirstCacheInstance() {
        val cache: ChronoDBCache = HeadFirstCache(100, null, null)
        Assert.assertNotNull(cache)
    }

    @Test
    fun cacheAndGetAreConsistent() {
        val cache = HeadFirstCache(100, null, null)
        val branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
        val key = QualifiedKey.createInDefaultKeyspace("Hello")
        cache.cache(branch, GetResult.create(key, "World", Period.createRange(100, 200)))
        cache.cache(branch, GetResult.create(key, "Foo", Period.createRange(200, 500)))
        run {
            // below lowest entry
            val result = cache.get<Any>(branch, 99, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isMiss)
        }
        run {
            // at lower bound of lowest entry
            val result = cache.get<Any>(branch, 100, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("World", result.value)
        }
        run {
            // in between bounds
            val result = cache.get<Any>(branch, 150, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("World", result.value)
        }
        run {
            // at upper bound of lowest entry
            val result = cache.get<Any>(branch, 199, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("World", result.value)
        }
        run {
            // at lower bound of upper entry
            val result = cache.get<Any>(branch, 200, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("Foo", result.value)
        }
        run {
            // in between bounds
            val result = cache.get<Any>(branch, 300, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("Foo", result.value)
        }
        run {
            // at upper bound of upper entry
            val result = cache.get<Any>(branch, 499, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isHit)
            Assert.assertEquals("Foo", result.value)
        }
        run {
            // outside of any entry
            val result = cache.get<Any>(branch, 550, key)
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isMiss)
        }
    }

    @Test
    fun cacheGetOnNonExistingRowDoesntCrash() {
        val cache = HeadFirstCache(1, null, null)
        val branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
        val result = cache.get<Any>(branch, 1234, QualifiedKey.createInDefaultKeyspace("Fake"))
        Assert.assertNotNull(result)
        Assert.assertTrue(result.isMiss)
    }


    @Test
    fun headFirstEvictionStrategyWorks() {
        val master = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
        val branch = "branch"
        val head = Period.createOpenEndedRange(100)
        val old = Period.createRange(10, 50)
        val cache = HeadFirstCache(8, master, "vertex")
        val result01 = GetResult.create(QualifiedKey.create("vertex", "K1"), "Result1", head)
        val result02 = GetResult.create(QualifiedKey.create("edge", "K2"), "Result2", head)
        val result03 = GetResult.create(QualifiedKey.create("vertex", "K3"), "Result3", old)
        val result04 = GetResult.create(QualifiedKey.create("edge", "K4"), "Result4", old)
        val result05 = GetResult.create(QualifiedKey.create("vertex", "K5"), "Result5", head)
        val result06 = GetResult.create(QualifiedKey.create("edge", "K6"), "Result6", head)
        val result07 = GetResult.create(QualifiedKey.create("vertex", "K7"), "Result7", old)
        val result08 = GetResult.create(QualifiedKey.create("edge", "K8"), "Result8", old)
        val result09 = GetResult.create(QualifiedKey.create("vertex", "K9"), "Result9", head)
        val result10 = GetResult.create(QualifiedKey.create("edge", "K10"), "Result10", head)
        val result11 = GetResult.create(QualifiedKey.create("vertex", "K11"), "Result11", head)

        cache.cache(branch, result06)
        cache.cache(branch, result07)
        cache.cache(branch, result08)
        cache.cache(master, result09)
        cache.cache(master, result10)
        cache.cache(master, result01)
        cache.cache(master, result02)
        cache.cache(master, result03)
        cache.cache(master, result04)
        cache.cache(branch, result05)
        Assert.assertEquals(8, cache.size())

        cache.cache(master, result11)

        Assert.assertEquals(8, cache.size())
        Assert.assertTrue(cache.get<Any>(branch, 110, QualifiedKey.create("edge", "K6")).isMiss)
        Assert.assertTrue(cache.get<Any>(branch, 20, QualifiedKey.create("vertex", "K7")).isMiss)
        Assert.assertTrue(cache.get<Any>(branch, 20, QualifiedKey.create("edge", "K8")).isMiss)
        Assert.assertTrue(cache.get<Any>(master, 110, QualifiedKey.create("vertex", "K9")).isHit)
        Assert.assertTrue(cache.get<Any>(master, 110, QualifiedKey.create("edge", "K10")).isHit)
        Assert.assertTrue(cache.get<Any>(master, 110, QualifiedKey.create("vertex", "K11")).isHit)

        // rollback the entire thing
        cache.rollbackToTimestamp(0)
        assertEquals(0, cache.size())
        // make sure the internal maps have been cleared up
        assertTrue(cache.checkIfDataStructuresAreClean())
    }

    @Test
    fun cacheSizeIsCorrect() {
        val qKeyA = QualifiedKey.createInDefaultKeyspace("a")
        val qKeyB = QualifiedKey.createInDefaultKeyspace("b")
        val qKeyC = QualifiedKey.createInDefaultKeyspace("c")
        val qKeyD = QualifiedKey.createInDefaultKeyspace("d")
        val cache = HeadFirstCache(3, null, null)
        cache.cache("master", GetResult.create(qKeyA, "Hello", Period.createRange(100, 200)))
        cache.cache("master", GetResult.create(qKeyA, "World", Period.createRange(200, 300)))
        cache.cache("master", GetResult.create(qKeyA, "Foo", Period.createRange(300, 400)))
        cache.cache("master", GetResult.create(qKeyA, "Bar", Period.createRange(400, 500)))
        cache.cache("master", GetResult.create(qKeyB, "Hello", Period.createRange(100, 200)))
        cache.cache("master", GetResult.create(qKeyB, "World", Period.createRange(200, 300)))
        Assert.assertEquals(3, cache.size())
        Assert.assertEquals(3, cache.statistics.evictionCount)
        Assert.assertEquals(0, cache.statistics.clearCount)
        cache.clear()
        Assert.assertEquals(0, cache.size().toLong())
        Assert.assertEquals(1, cache.statistics.clearCount)
        cache.cache("master", GetResult.create(qKeyA, "Hello", Period.createRange(100, 200)))
        cache.cache("master", GetResult.create(qKeyA, "World", Period.createRange(200, 300)))
        cache.cache("master", GetResult.create(qKeyA, "Foo", Period.createRange(300, 400)))
        cache.writeThrough("master", 400, qKeyA, "Bar")
        cache.writeThrough("master", 0, qKeyB, "Hello")
        cache.writeThrough("master", 100, qKeyB, "World")
        cache.writeThrough("master", 100, qKeyC, "World")
        cache.writeThrough("master", 200, qKeyC, "Foo")
        cache.cache("master", GetResult.create(qKeyC, "Bar", Period.createRange(300, 400)))
        cache.writeThrough("master", 100, qKeyD, "World")
        cache.writeThrough("master", 200, qKeyD, "Foo")
        cache.cache("master", GetResult.create(qKeyD, "Bar", Period.createRange(300, 400)))
        Assert.assertEquals(3, cache.size())
        Assert.assertEquals(12, cache.statistics.evictionCount)
        Assert.assertEquals(0, cache.statistics.rollbackCount)
        cache.rollbackToTimestamp(0)
        Assert.assertEquals(0, cache.size())
        Assert.assertEquals(12, cache.statistics.evictionCount)
        Assert.assertEquals(1, cache.statistics.rollbackCount)
    }

    @Test
    @Ignore
    fun performanceTest() {
        println("Preparing data...")
        val keys = (0..1_000_000).asSequence().map {
            QualifiedKey.create("vertex", UUID.randomUUID().toString())
        }.toList()

        val cache = MosaicCache(200_000)
        // val cache = HeadFirstCache(200_000, null, null)

        measureNanoTime {
            for (key in keys) {
                cache.cache("master", GetResult.create(key, key.toString(), Period.createOpenEndedRange(100)))
            }
        }.let { println("Insertion took: ${TimeUnit.NANOSECONDS.toMillis(it)} ms") }
        repeat(10) {
            measureNanoTime {
                for (key in keys) {
                    cache.get<String>("master", 110, key)
                }
            }.let { println("Lookup took: ${TimeUnit.NANOSECONDS.toMillis(it)} ms") }
        }

        assertEquals(cache.computedSize(), cache.size())
        println("Cache Size: ${cache.size()}")
    }
}