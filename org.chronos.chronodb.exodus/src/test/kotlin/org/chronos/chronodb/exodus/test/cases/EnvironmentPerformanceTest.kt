package org.chronos.chronodb.exodus.test.cases

import com.google.common.collect.Iterators
import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.chronos.common.test.utils.NamedPayload
import org.chronos.common.test.utils.TestUtils
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime


class EnvironmentPerformanceTest : EnvironmentTest() {

    @Test
    fun runTest() {
        val totalEntryCount = 100_000
        val uniqueKeyCount = 10_000
        val numberOfCommits = 1000
        val entriesPerCommit = totalEntryCount / numberOfCommits

        val numberOfGets = 200_000

        println("EXODUS")
        println("Total entry count: ${totalEntryCount}")
        println("Unique Key count: ${uniqueKeyCount}")
        println("Number of commits: ${numberOfCommits}")

        val uniqueIds = (0 until uniqueKeyCount).asSequence().map { UUID.randomUUID().toString() }.toList()
        val keyIterator = Iterators.cycle(uniqueIds)

        var insertTime = 0L
        val timestampLowerbound = System.currentTimeMillis()
        repeat(numberOfCommits) { commitIndex ->
            val dataSet = mutableMapOf<String, ByteArray>()
            repeat(entriesPerCommit) {
                val key = keyIterator.next()
                val value = NamedPayload.create10KB().payload
                dataSet[key] = value
            }
            measureNanoTime {
                performPut(environment, System.currentTimeMillis(), dataSet)
            }.let {
                insertTime += it
            }
        }
        val timestampUpperbound = System.currentTimeMillis()
        println("Total insertion time: ${TimeUnit.NANOSECONDS.toMillis(insertTime)}ms")

        var totalLength = 0L
        var queryTimeNanos = 0L
        repeat(numberOfGets) { i ->
            val timestamp = TestUtils.randomBetween(timestampLowerbound, timestampUpperbound)
            val requestKey = TestUtils.getRandomEntryOf(uniqueIds)
            var result: ByteIterable? = null
            queryTimeNanos += measureNanoTime {
                result = performGet(environment, requestKey, timestamp)
            }
            if (result != null) {
                totalLength += result!!.length
            }
        }
        println("${numberOfGets} Get queries took ${TimeUnit.NANOSECONDS.toMillis(queryTimeNanos)}ms")
        println("The results have a combined size of ${FileUtils.byteCountToDisplaySize(totalLength)}")
        var count = 0
        var size = 0L
        measureNanoTime {
            val result = count(environment)
            count = result.first
            size = result.second
        }.let { println("A full table scan took ${TimeUnit.NANOSECONDS.toMillis(it)}ms") }
        println("The database has ${count} entries with a combined value size of ${FileUtils.byteCountToDisplaySize(size)}")
    }
}

private fun performGet(environment: Environment, key: String, timestamp: Long): ByteIterable? {
    return environment.computeInReadonlyTransaction { tx ->
        val tKey = UnqualifiedTemporalKey(key, timestamp).toByteIterable()
        val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
        store.openCursor(tx).use { cursor ->
            val value = cursor.getSearchKeyRange(tKey)
            if (value == null) {
                // there is no greater/equal entry, check the last
                if (!cursor.last) {
                    // the database is empty
                    return@computeInReadonlyTransaction null
                }
                // the last entry is the largest entry in the database
                // which is less than or equal to the given key/timestamp.
                return@computeInReadonlyTransaction cursor.value
            } else {
                // we ended up at a valid entry, see if it is equal
                if (cursor.key == tKey) {
                    // we found the exact key
                    return@computeInReadonlyTransaction cursor.value
                } else {
                    // we found the "greater than" key, move one to the left
                    if (!cursor.prev) {
                        // we "fell off" the left end of the B-Tree; the request
                        // key is smaller than the smallest key in the tree.
                        return@computeInReadonlyTransaction null
                    } else {
                        // we arrived at our goal
                        return@computeInReadonlyTransaction cursor.value
                    }
                }
            }
        }
    }
}

private fun performPut(environment: Environment, timestamp: Long, dataSet: Map<String, ByteArray?>) {
    environment.executeInExclusiveTransaction { tx ->
        dataSet.forEach { key, value ->
            val tKey = UnqualifiedTemporalKey(key, timestamp).toByteIterable()
            val realValue = ArrayByteIterable(value
                ?: ByteArray(0))
            val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
            store.put(tx, tKey, realValue)
        }
        tx.commit()
    }
}

private fun count(environment: Environment): Pair<Int, Long> {
    return environment.computeInReadonlyTransaction { tx ->
        val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
        var count = 0
        var size = 0L
        store.openCursor(tx).use { cursor ->
            while (cursor.next) {
                count++
                size += cursor.value.length
            }
        }
        Pair(count, size)
    }
}