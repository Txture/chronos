package org.chronos.chronodb.exodus.test.cases

import jetbrains.exodus.ByteIterable
import org.apache.commons.io.FileUtils
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.kotlin.ext.parseAsInverseUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.parseAsUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.system.measureNanoTime


class TemporalKeySerializationTest {

    @Test
    @Tag("performance")
    fun testSerializationPerformance() {
        runTest(
            blocks = 10,
            keysPerBlock = 100000,
            serialize = UnqualifiedTemporalKey::toByteIterable,
            deserialize = ByteIterable::parseAsUnqualifiedTemporalKey
        )
    }

    @Test
    fun serializationPreservesSortOrder() {
        val dataSet = listOf(
            UnqualifiedTemporalKey.create("a", 1),
            UnqualifiedTemporalKey.create("a", 100),
            UnqualifiedTemporalKey.create("a", 500),
            UnqualifiedTemporalKey.create("a", 1000),
            UnqualifiedTemporalKey.create("aa", 1),
            UnqualifiedTemporalKey.create("a", 100),
            UnqualifiedTemporalKey.create("a", 500),
            UnqualifiedTemporalKey.create("a", 1000),
            UnqualifiedTemporalKey.create("b", 1),
            UnqualifiedTemporalKey.create("b", 100),
            UnqualifiedTemporalKey.create("b", 500),
            UnqualifiedTemporalKey.create("b", 1000),
            UnqualifiedTemporalKey.create("c", 1),
            UnqualifiedTemporalKey.create("c", 100),
            UnqualifiedTemporalKey.create("c", 500),
            UnqualifiedTemporalKey.create("c", 1000)
        ).shuffled()

        // sort a copy of the data set based on the binary format
        val dataSetBinarySorted = dataSet.asSequence()
            .map(UnqualifiedTemporalKey::toByteIterable)
            .sorted()
            .map(ByteIterable::parseAsUnqualifiedTemporalKey)
            .toList()

        // sort the original data set based on the object representation
        val dataSetObjectSorted = dataSet.sorted()

        // assert that the sort orders are the same
        for (i in 0 until dataSetObjectSorted.size) {
            dataSetObjectSorted[i] shouldBe dataSetBinarySorted[i]
        }
    }

    @Test
    fun serializationPreservesSortOrderForInverseKeys() {
        val dataSet = listOf(
            InverseUnqualifiedTemporalKey.create(1, "a"),
            InverseUnqualifiedTemporalKey.create(100, "a"),
            InverseUnqualifiedTemporalKey.create(500, "a"),
            InverseUnqualifiedTemporalKey.create(1000, "a"),
            InverseUnqualifiedTemporalKey.create(1, "aa"),
            InverseUnqualifiedTemporalKey.create(100, "a"),
            InverseUnqualifiedTemporalKey.create(500, "a"),
            InverseUnqualifiedTemporalKey.create(1000, "a"),
            InverseUnqualifiedTemporalKey.create(1, "b"),
            InverseUnqualifiedTemporalKey.create(100, "b"),
            InverseUnqualifiedTemporalKey.create(500, "b"),
            InverseUnqualifiedTemporalKey.create(1000, "b"),
            InverseUnqualifiedTemporalKey.create(1, "c"),
            InverseUnqualifiedTemporalKey.create(100, "c"),
            InverseUnqualifiedTemporalKey.create(500, "c"),
            InverseUnqualifiedTemporalKey.create(1000, "c")
        ).shuffled()

        // sort a copy of the data set based on the binary format
        val dataSetBinarySorted = dataSet.asSequence()
            .map(InverseUnqualifiedTemporalKey::toByteIterable)
            .sorted()
            .map(ByteIterable::parseAsInverseUnqualifiedTemporalKey)
            .toList()

        // sort the original data set based on the object representation
        val dataSetObjectSorted = dataSet.sorted()

        // assert that the sort orders are the same
        for (i in 0 until dataSetObjectSorted.size) {
            dataSetObjectSorted[i] shouldBe dataSetBinarySorted[i]
        }
    }

    private fun runTest(blocks: Int, keysPerBlock: Int, serialize: (UnqualifiedTemporalKey) -> ByteIterable, deserialize: (ByteIterable) -> UnqualifiedTemporalKey) {
        var totalLength = 0L
        var totalTimeNanos = 0L

        repeat(blocks) {
            val data = (0 until keysPerBlock).asSequence().map {
                UnqualifiedTemporalKey.create(UUID.randomUUID().toString(), System.currentTimeMillis())
            }.toList()
            measureNanoTime {
                for (tKey in data) {
                    val asBytes = serialize(tKey)
                    totalLength += asBytes.length
                    val asTKey = deserialize(asBytes)
                    tKey shouldBe asTKey
                }
            }.let { time -> totalTimeNanos += time }
        }

        println("Converting ${blocks * keysPerBlock} UnqualifiedTemporalKeys (ser+deser+compare) in memory" +
            "took ${TimeUnit.NANOSECONDS.toMillis(totalTimeNanos)}ms and " +
            "produced ${FileUtils.byteCountToDisplaySize(totalLength)} of data.")
    }

}