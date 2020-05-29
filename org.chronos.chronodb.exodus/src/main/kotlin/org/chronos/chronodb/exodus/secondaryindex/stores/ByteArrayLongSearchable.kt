package org.chronos.chronodb.exodus.secondaryindex.stores

import jetbrains.exodus.ByteIterable
import java.nio.ByteBuffer

class ByteIterableLongSearchable : LongSearchable {

    private val bytes: ByteArray
    private val byteSize: Int
    private val buffer: ByteBuffer

    constructor(byteIterable: ByteIterable){
        this.bytes = byteIterable.bytesUnsafe
        this.byteSize = byteIterable.length
        this.buffer = ByteBuffer.wrap(this.bytes)
    }

    override val size: Int
        get() = this.byteSize / java.lang.Long.BYTES


    override fun get(index: Int): Long {
        return this.buffer.getLong(index * java.lang.Long.BYTES)
    }
}