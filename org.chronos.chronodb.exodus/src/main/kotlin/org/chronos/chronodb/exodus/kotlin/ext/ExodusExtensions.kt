package org.chronos.chronodb.exodus.kotlin.ext

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.*
import jetbrains.exodus.util.ByteIterableUtil
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import java.nio.ByteBuffer
import java.util.UUID



// =================================================================================================================
// CURSOR
// =================================================================================================================

fun Cursor.floorEntry(key: ByteIterable): Pair<ByteIterable, ByteIterable>? {
    val value = this.getSearchKeyRange(key)
    if (value != null) {
        if (ByteIterableUtil.compare(this.key, key) == 0) {
            return Pair(this.key, value)
        }
        if (this.prev) {
            return Pair(this.key, this.value)
        } else {
            // no previous K/V pair exists in the store
            return null
        }
    } else {
        // the key is greater than the last (rightmost) key in the store
        if (this.last) {
            return Pair(this.key, this.value)
        } else {
            return null
        }
    }
}

fun Cursor.floorValue(key: ByteIterable): ByteIterable? {
    return this.floorEntry(key)?.second
}

fun Cursor.floorKey(key: ByteIterable): ByteIterable? {
    return this.floorEntry(key)?.first
}

fun Cursor.lowerEntry(key: ByteIterable): Pair<ByteIterable, ByteIterable>? {
    val floorEntry = this.floorEntry(key)
    if (floorEntry == null) {
        // there is no entry less than or equal to the given one,
        // so there can't be an entry strictly less than the given one
        return null
    }
    if (ByteIterableUtil.compare(floorEntry.first, key) == 0) {
        // the floor key is the current key, to find the next lower
        // one we move to previous
        if (this.prev) {
            return Pair(this.key, this.value)
        } else {
            // there is no previous
            return null
        }
    } else {
        // the floor key is already lower (the exact key was not in the store)
        return floorEntry
    }
}

fun Cursor.lowerKey(key: ByteIterable): ByteIterable? {
    return this.lowerEntry(key)?.first
}

fun Cursor.lowerValue(key: ByteIterable): ByteIterable? {
    return this.lowerEntry(key)?.second
}

fun Cursor.ceilEntry(key: ByteIterable): Pair<ByteIterable, ByteIterable>? {
    val value = this.getSearchKeyRange(key)
    if (value == null) {
        return null
    }
    return Pair(this.key, this.value)
}

fun Cursor.ceilKey(key: ByteIterable): ByteIterable? {
    return this.ceilEntry(key)?.first
}

fun Cursor.ceilValue(key: ByteIterable): ByteIterable? {
    return this.ceilEntry(key)?.second
}

fun Cursor.higherEntry(key: ByteIterable): Pair<ByteIterable, ByteIterable>? {
    val ceilEntry = this.ceilEntry(key)
    if (ceilEntry == null) {
        // there is no greater/equal key, so there is no greater key either
        return null
    }
    if (ByteIterableUtil.compare(ceilEntry.first, key) == 0) {
        // the ceil key is the search key -> the next-greater key is the higher key
        if (this.next) {
            return Pair(this.key, this.value)
        } else {
            // no higher key in store
            return null
        }
    } else {
        // the ceil entry key is already greater than the search key
        return ceilEntry
    }
}

fun Cursor.higherKey(key: ByteIterable): ByteIterable? {
    return this.higherEntry(key)?.first
}

fun Cursor.higherValue(key: ByteIterable): ByteIterable? {
    return this.higherEntry(key)?.second
}

fun Cursor.floorAndHigherEntry(key: ByteIterable): Pair<Pair<ByteIterable, ByteIterable>?, Pair<ByteIterable, ByteIterable>?> {
    val value = this.getSearchKeyRange(key)
    if (value == null) {
        // the request key is greater than the right-most key in the store
        if (this.last) {
            val floorEntry = Pair(this.key, this.value)
            return Pair(floorEntry, null)
        } else {
            // store is empty
            return Pair(null, null)
        }
    } else {
        val ceilKey = this.key
        if (ByteIterableUtil.compare(ceilKey, key) == 0) {
            // this is an exact match with the key we are looking for, so we
            // use it as floor key
            val floorEntry = Pair(this.key, this.value)
            // the higher entry is the one to the right (if any)
            if (this.next) {
                return Pair(floorEntry, Pair(this.key, this.value))
            } else {
                // this is the largest key in the store
                return Pair(floorEntry, null)
            }
        } else {
            // the ceil key is the higher key, check the previous one for the lower key
            val higherEntry = Pair(this.key, this.value)
            if (this.prev) {
                return Pair(Pair(this.key, this.value), higherEntry)
            } else {
                // the higher entry is the leftmost entry in the store
                return Pair(null, higherEntry)
            }
        }
    }
}

// =================================================================================================================
// ENVIRONMENT
// =================================================================================================================

fun <T> Environment.readFrom(storeName: String, config: StoreConfig, consumer: (Cursor) -> T): T {
    return this.computeInReadonlyTransaction { tx ->
        val store = this.openStore(storeName, config, tx)
        store.openCursor(tx).use(consumer)
    }
}

// =================================================================================================================
// STORE
// =================================================================================================================

fun Store.get(tx: Transaction, key: String): ByteIterable? {
    return this.get(tx, key.toByteIterable())
}

fun Store.put(tx: Transaction, key: String, value: ByteIterable): Boolean {
    return this.put(tx, key.toByteIterable(), value)
}

// =================================================================================================================
// BYTE ITERABLE
// =================================================================================================================


fun ByteIterable.toByteArray(): ByteArray {
    val bytesUnsafe = this.bytesUnsafe
    if(bytesUnsafe.size == this.length){
        return bytesUnsafe
    }else{
        // create a shortened copy
        val array = ByteArray(this.length)
        System.arraycopy(bytesUnsafe, 0, array, 0, this.length)
        return array
    }
}

fun ByteArray.toByteIterable(): ByteIterable {
    return ArrayByteIterable(this)
}

fun Boolean.toByteIterable(): ByteIterable {
    return BooleanBinding.booleanToEntry(this)
}

fun UnqualifiedTemporalKey.toByteIterable(): ByteIterable {
    // the byte-array form looks like this:
    //
    // [user key string bytes][8 bytes for timestamp (long)]
    //
    // This preserves the comparison ordering.
    val userKey = StringBinding.stringToEntry(this.key)
    val time = LongBinding.longToEntry(this.timestamp)
    val jointArray = ByteArray(userKey.length + time.length)
    System.arraycopy(userKey.bytesUnsafe, 0, jointArray, 0, userKey.length)
    System.arraycopy(time.bytesUnsafe, 0, jointArray, userKey.length, time.length)
    return ArrayByteIterable(jointArray)
}

fun InverseUnqualifiedTemporalKey.toByteIterable(): ByteIterable {
    // the byte-array form looks like this:
    //
    // [8 bytes for timestamp (long)][user key string bytes]
    //
    // This preserves the comparison ordering.
    val time = LongBinding.longToEntry(this.timestamp)
    val userKey = StringBinding.stringToEntry(this.key)
    val jointArray = ByteArray(userKey.length + time.length)
    // first 8 bytes store the timestamp
    System.arraycopy(time.bytesUnsafe, 0, jointArray, 0, 8)
    // remaining bytes store the user key
    System.arraycopy(userKey.bytesUnsafe, 0, jointArray, 8, userKey.length)
    return ArrayByteIterable(jointArray)
}

fun ByteIterable.parseAsUnqualifiedTemporalKey(): UnqualifiedTemporalKey {
    // the byte-array form looks like this:
    //
    // [user key string bytes][8 bytes for timestamp (long)]
    //
    val timePart = this.subIterable(this.length - 8, 8)
    val timestamp = LongBinding.entryToLong(timePart)
    val keyPart = this.subIterable(0, this.length - 8)
    val key = StringBinding.entryToString(keyPart)
    return UnqualifiedTemporalKey.create(key, timestamp)
}

fun ByteIterable.parseAsInverseUnqualifiedTemporalKey(): InverseUnqualifiedTemporalKey {
    // the byte-array form looks like this:
    //
    // [8 bytes for timestamp (long)][user key string bytes]
    val timestamp = LongBinding.entryToLong(this.subIterable(0, 8))
    val key = StringBinding.entryToString(this.subIterable(8, this.length - 8))
    return InverseUnqualifiedTemporalKey.create(timestamp, key)
}

fun ByteIterable.isEmpty(): Boolean {
    return !this.iterator().hasNext()
}

fun Long.toByteIterable(): ByteIterable {
    return LongBinding.longToEntry(this)
}

fun ByteIterable.parseAsLong(): Long {
    return LongBinding.entryToLong(this)
}

fun String.toByteIterable(): ByteIterable {
    return StringBinding.stringToEntry(this)
}

fun ByteIterable.parseAsString(): String {
    return StringBinding.entryToString(this)
}

fun ByteIterable.parseAsDouble(): Double {
    return DoubleBinding.entryToDouble(this)
}

fun Double.toByteIterable(): ByteIterable {
    return DoubleBinding.doubleToEntry(this)
}

fun ByteIterable.parseAsUUID(): UUID {
    val bb = ByteBuffer.wrap(this.toByteArray())
    val firstLong = bb.getLong()
    val secondLong = bb.getLong()
    return UUID(firstLong, secondLong)
}

fun UUID.toByteIterable(): ByteIterable {
    val bb = ByteBuffer.wrap(ByteArray(16))
    bb.putLong(this.mostSignificantBits)
    bb.putLong(this.leastSignificantBits)
    return bb.array().toByteIterable()
}

fun ByteIterable.consistsOfZeroes(): Boolean {
    val byteIterator = this.iterator()
    while(byteIterator.hasNext()) {
        val next = byteIterator.next()
        if(next != 0.toByte()) {
            return false
        }
    }
    return true
}