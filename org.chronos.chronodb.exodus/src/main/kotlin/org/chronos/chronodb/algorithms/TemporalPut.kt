package org.chronos.chronodb.algorithms

import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import java.util.*

// =================================================================================================================
// PUBLIC API
// =================================================================================================================

fun temporalPut(timestamp: Long, contents: Map<String, ByteArray>, receiver: (List<TemporalEntry>)->Unit){
    require(timestamp >= 0) { "Argument 'timestamp' must not be negative!" }
    val batch = contents.asSequence()
            .map { createTemporalEntry(it.key, it.value, timestamp) }
            .toList()
    receiver(batch)
}

fun temporalPut(timestamp: Long, contents: Map<String, ByteArray>, receiver: PutDataReceiver){
    temporalPut(timestamp, contents){ entries ->
        receiver.store(entries)
    }
}

// =================================================================================================================
// DATA STRUCTURES
// =================================================================================================================

@FunctionalInterface
interface PutDataReceiver {

    fun store(entries: List<TemporalEntry>)

}

interface PutDataSingleReceiver : PutDataReceiver {

    fun store(key: UnqualifiedTemporalKey, value: ByteArray, inverseKey: InverseUnqualifiedTemporalKey, inverseValue: Boolean)

    fun store(entry: TemporalEntry){
        store(entry.key, entry.value, entry.inverseKey, entry.inverseValue)
    }
    override fun store(entries: List<TemporalEntry>){
        entries.forEach(this::store)
    }

}

data class TemporalEntry(
        val key: UnqualifiedTemporalKey,
        val value: ByteArray,
        val inverseKey: InverseUnqualifiedTemporalKey,
        val inverseValue: Boolean
){

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as TemporalEntry
        if (key != other.key) return false
        if (!Arrays.equals(value, other.value)) return false
        if (inverseKey != other.inverseKey) return false
        if (inverseValue != other.inverseValue) return false
        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + Arrays.hashCode(value)
        result = 31 * result + inverseKey.hashCode()
        result = 31 * result + inverseValue.hashCode()
        return result
    }
}

// =================================================================================================================
// HELPER FUNCTIONS
// =================================================================================================================

private fun createTemporalEntry(key: String, value: ByteArray?, timestamp: Long): TemporalEntry {
    val tKey = UnqualifiedTemporalKey.create(key, timestamp)
    val inverseKey = InverseUnqualifiedTemporalKey.create(timestamp, key)
    val inverseValue = value != null
    return TemporalEntry(tKey, value.nullToZeroLength(), inverseKey, inverseValue)
}

fun ByteArray?.nullToZeroLength(): ByteArray{
    if(this == null){
        return ByteArray(0)
    }else{
        return this
    }
}