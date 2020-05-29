package org.chronos.chronodb.exodus.util

import com.google.common.collect.Iterables
import java.nio.ByteBuffer

fun writeInt(buffer: ByteArray, value: Int, offset: Int) {
    ByteBuffer.wrap(buffer, offset, buffer.size - offset).putInt(value)
}

fun readInt(buffer: ByteArray, offset: Int): Int {
    return ByteBuffer.wrap(buffer, offset, buffer.size - offset).int
}

fun writeLong(buffer: ByteArray, value: Long, offset: Int) {
    ByteBuffer.wrap(buffer, offset, buffer.size - offset).putLong(value)
}

fun readLong(buffer: ByteArray, offset: Int): Long {
    return ByteBuffer.wrap(buffer, offset, buffer.size - offset).long
}

fun writeLongsToBytes(values: Iterable<Long>): ByteArray {
    val byteArray = ByteArray(Iterables.size(values) * java.lang.Long.BYTES)
    writeLongsToBytes(byteArray, values)
    return byteArray
}

fun writeLongsToBytes(buffer: ByteArray, values: Iterable<Long>) {
    val buf = ByteBuffer.wrap(buffer)
    values.forEach{ buf.putLong(it) }
}

fun readLongsFromBytes(buffer: ByteArray): MutableList<Long> {
    val resultList = mutableListOf<Long>()
    val buf = ByteBuffer.wrap(buffer)
    while(buf.hasRemaining()){
        val long = buf.long
        resultList.add(long)
    }
    return resultList
}