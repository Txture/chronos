package org.chronos.chronodb.exodus.test.cases.secondaryindex

import com.google.common.collect.DiscreteDomain.longs
import jetbrains.exodus.ArrayByteIterable
import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.common.testing.kotlin.ext.beNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.util.writeLongsToBytes
import org.junit.jupiter.api.Test

class RangeBinarySearchTest {

    @Test
    fun worksWithTimestampsOutOfRange() {
        rangeBinarySearch(longs(10, 20), 1) should beNull()
        rangeBinarySearch(longs(10, 20, 30, 40), 1) should beNull()

        rangeBinarySearch(longs(30, 40), 100) should beNull()
        rangeBinarySearch(longs(10, 20, 30, 40), 100) should beNull()
    }

    @Test
    fun worksWithEmptyList(){
        rangeBinarySearch(longs(), 1234) should beNull()
    }

    @Test
    fun worksWithEmptyListOnBytes(){
        rangeBinarySearch(bytes(), 1234) should beNull()
    }

    @Test
    fun worksWithSingleRange(){
        rangeBinarySearch(longs(1000, 2000), 1432) shouldBe Pair(1000L, 2000L)
        rangeBinarySearch(longs(1000, 2000), 1000) shouldBe Pair(1000L, 2000L)
        rangeBinarySearch(longs(1000, 2000), 2000) should beNull()
        rangeBinarySearch(longs(1000, 2000), 2001) should beNull()
        rangeBinarySearch(longs(1000, 2000), 999) should beNull()
    }


    @Test
    fun worksWithSingleRangeOnBytes(){
        rangeBinarySearch(bytes(1000, 2000), 1432) shouldBe Pair(1000L, 2000L)
        rangeBinarySearch(bytes(1000, 2000), 1000) shouldBe Pair(1000L, 2000L)
        rangeBinarySearch(bytes(1000, 2000), 2000) should beNull()
        rangeBinarySearch(bytes(1000, 2000), 2001) should beNull()
        rangeBinarySearch(bytes(1000, 2000), 999) should beNull()
    }

    @Test
    fun worksWithHoles(){
        rangeBinarySearch(longs(
                1000, 2000,
                5000, 7000,
                8500, 9600
        ), 3522) should beNull()
        rangeBinarySearch(longs(
                1000, 2000,
                5000, 7000,
                8500, 9600
        ), 8600) shouldBe Pair(8500L, 9600L)
    }

    @Test
    fun worksWithHolesOnBytes(){
        rangeBinarySearch(bytes(
                1000, 2000,
                5000, 7000,
                8500, 9600
        ), 3522) should beNull()
        rangeBinarySearch(bytes(
                1000, 2000,
                5000, 7000,
                8500, 9600
        ), 8600) shouldBe Pair(8500L, 9600L)
    }


    @Test
    fun worksWhenMiddleRangeIsTheResult(){
        rangeBinarySearch(longs(
                10, 20,
                50, 70,
                80, 100
        ), 60) shouldBe Pair(50L,70L)
    }


    @Test
    fun worksWhenMiddleRangeIsTheResultOnBytes(){
        rangeBinarySearch(bytes(
                10, 20,
                50, 70,
                80, 100
        ), 60) shouldBe Pair(50L,70L)
    }

    @Test
    fun canFindTerminatedRanges(){
        rangeBinarySearchForHighestTerminatedPeriod(bytes(), 1) should beNull()
        val bytes2 = bytes(
                10, 20
        )
        rangeBinarySearchForHighestTerminatedPeriod(bytes2, 9) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes2, 10) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes2, 15) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes2, 20) shouldBe Pair(10L, 20L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes2, 21) shouldBe Pair(10L, 20L)
        val bytes3 = bytes(
                10, 20,
                50, 70,
                80, 100
        )
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 60) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 9) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 15) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 20) shouldBe Pair(10L, 20L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 30) shouldBe Pair(10L, 20L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 50) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 70) shouldBe Pair(50L, 70L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 75) shouldBe Pair(50L, 70L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 80) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 90) should beNull()
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 100) shouldBe Pair(80L, 100L)
        rangeBinarySearchForHighestTerminatedPeriod(bytes3, 110) shouldBe Pair(80L, 100L)
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun longs(vararg values: Long): LongSearchable {
        return ListLongSearchable(values.asList())
    }

    private fun bytes(vararg values: Long): LongSearchable {
        return ByteIterableLongSearchable(ArrayByteIterable(writeLongsToBytes(values.toList())))
    }
}