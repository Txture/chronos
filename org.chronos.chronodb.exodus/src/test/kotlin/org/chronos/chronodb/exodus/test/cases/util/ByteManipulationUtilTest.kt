package org.chronos.chronodb.exodus.test.cases.util

import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.util.*
import org.junit.jupiter.api.Test

class ByteManipulationUtilTest {

    @Test
    fun canWriteAndReadInts(){
        val size = Integer.BYTES
        val bytes = ByteArray(size * 5)
        writeInt(bytes, 12345678, 0)
        writeInt(bytes, 45678910, size)
        writeInt(bytes, -4467810, size*2)
        writeInt(bytes, Int.MAX_VALUE, size*3)
        writeInt(bytes, Int.MIN_VALUE, size*4)
        readInt(bytes, 0) shouldBe 12345678
        readInt(bytes, size) shouldBe 45678910
        readInt(bytes, size*2) shouldBe -4467810
        readInt(bytes, size*3) shouldBe Int.MAX_VALUE
        readInt(bytes, size*4) shouldBe Int.MIN_VALUE
    }

    @Test
    fun canWriteAndReadLongs(){
        val size = java.lang.Long.BYTES
        val bytes = ByteArray(size * 5)
        writeLong(bytes, 12345678, 0)
        writeLong(bytes, 45678910, size)
        writeLong(bytes, -4467810, size*2)
        writeLong(bytes, Long.MAX_VALUE, size*3)
        writeLong(bytes, Long.MIN_VALUE, size*4)
        readLong(bytes, 0) shouldBe 12345678
        readLong(bytes, size) shouldBe 45678910
        readLong(bytes, size*2) shouldBe -4467810
        readLong(bytes, size*3) shouldBe Long.MAX_VALUE
        readLong(bytes, size*4) shouldBe Long.MIN_VALUE
    }

    @Test
    fun canWriteAndReadListOfLongs(){
        val longs = listOf(123456, -232312312, Long.MAX_VALUE, Long.MIN_VALUE)
        val bytes = writeLongsToBytes(longs)
        val longs2 = readLongsFromBytes(bytes)
        longs2 shouldBe longs
    }
}