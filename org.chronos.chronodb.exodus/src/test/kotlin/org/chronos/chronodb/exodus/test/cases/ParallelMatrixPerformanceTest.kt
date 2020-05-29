package org.chronos.chronodb.exodus.test.cases

import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.junit.jupiter.api.Tag

@Tag("performance")
class ParallelMatrixPerformanceTest : EnvironmentTest() {

//    @Test
//    fun performanceForManyOverridesTest() {
//        val totalEntryCount = 100_000
//        val uniqueKeyCount = 10_000
//        val numberOfCommits = 1000
//        val entriesPerCommit = totalEntryCount / numberOfCommits
//
//        val numberOfGets = 200_000
//
//        println("EXODUS")
//        println("Total entry count: ${totalEntryCount}")
//        println("Unique Key count: ${uniqueKeyCount}")
//        println("Number of commits: ${numberOfCommits}")
//
//        val matrix = TemporalExodusMatrix(environment, "test", "default", System.currentTimeMillis())
//
//        val uniqueIds = (0 until uniqueKeyCount).asSequence().map { UUID.randomUUID().toString() }.toList()
//        val keyIterator = Iterators.cycle(uniqueIds)
//
//        var insertTime = 0L
//        val timestampLowerbound = System.currentTimeMillis()
//        repeat(numberOfCommits) { commitIndex ->
//            val dataSet = mutableMapOf<String, ByteArray>()
//            repeat(entriesPerCommit) {
//                val key = keyIterator.next()
//                val value = NamedPayload.create10KB().payload
//                dataSet[key] = value
//            }
//            measureNanoTime {
//                matrix.put(System.currentTimeMillis(), dataSet)
//            }.let {
//                insertTime += it
//            }
//        }
//        val timestampUpperbound = System.currentTimeMillis()
//        println("Total insertion time: ${TimeUnit.NANOSECONDS.toMillis(insertTime)}ms")
//
//        var totalLength = 0L
//        val queryTimeNanos = measureNanoTime {
//            totalLength = IntStream.range(0, numberOfGets).parallel().mapToLong{
//                val timestamp = TestUtils.randomBetween(timestampLowerbound, timestampUpperbound)
//                val requestKey = TestUtils.getRandomEntryOf(uniqueIds)
//                matrix.get(timestamp, requestKey)!!.value.let { if (it == null) 0 else it.size.toLong() }
//            }.sum()
//        }
//
//        println("${numberOfGets} Get queries took ${TimeUnit.NANOSECONDS.toMillis(queryTimeNanos)}ms")
//        val bytes = FileUtils.sizeOfDirectory(testDir)
//        println("Test Dir Size: ${FileUtils.byteCountToDisplaySize(bytes)}")
//        println("Test Dir Location: ${testDir.absolutePath}")
//    }
}