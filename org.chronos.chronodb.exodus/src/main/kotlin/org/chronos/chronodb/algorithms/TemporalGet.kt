package org.chronos.chronodb.algorithms

import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.exodus.kotlin.ext.mapSingle
import org.chronos.chronodb.internal.api.GetResult
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey

fun temporalGet(keyspace: String, key: String, timestamp: Long, floorEntry: Pair<UnqualifiedTemporalKey, ByteArray>?, higherEntry: Pair<UnqualifiedTemporalKey, ByteArray>?): GetResult<ByteArray> {
    require(keyspace.isNotEmpty()) { "Argument 'keyspace' must not be empty!" }
    require(key.isNotEmpty()) { "Argument 'key' must not be empty!" }
    require(timestamp >= 0) { "Argument 'timestamp' must not be negative!" }
    val qKey = QualifiedKey.create(keyspace, key)
    val floorKey = floorEntry.mapSingle { it.first }
    val ceilKey = higherEntry.mapSingle { it.first }
    if (floorEntry == null || floorKey?.key != key) {
        // we have no "next lower" bound -> we already know that the result will be empty.
        // now we need to check if we have an upper bound for the validity of our empty result...
        if (higherEntry == null || ceilKey?.key != key) {
            // there is no value for this key (at all, not at any timestamp)
            return GetResult.createNoValueResult(qKey, Period.eternal())
        } else if (ceilKey.key == key) {
            // there is no value for this key, until a certain timestamp is reached
            val period = Period.createRange(0, ceilKey.timestamp)
            return GetResult.createNoValueResult(qKey, period)
        }
    } else {
        // we have a "next lower" bound -> we already know that the result will be non-empty.
        val value = floorEntry.second.mapSingle(ByteArray::zeroLengthToNull)
        // now we need to check if we have an upper bound for the validity of our result...
        if (higherEntry == null || ceilKey?.key != key) {
            // there is no further value for this key, therefore we have an open-ended period
            val range = Period.createOpenEndedRange(floorKey.timestamp)
            return GetResult.create(qKey, value, range)
        } else if (ceilKey.key == key) {
            // the value of the result is valid between the floor and ceiling entries
            val floorTimestamp = floorKey.timestamp
            val ceilTimestamp = ceilKey.timestamp
            if (floorTimestamp >= ceilTimestamp) {
                throw IllegalStateException("Invalid 'getRanged' state - floor timestamp (${floorTimestamp} >= ceil timestamp (${ceilTimestamp})! " +
                    "Requested: '${key}@${timestamp}', floor: '${floorKey}', ceil: '${ceilKey}'.")
            }
            val period = Period.createRange(floorTimestamp, ceilTimestamp)
            return GetResult.create(qKey, value, period)
        }
    }
    // this code is effectively unreachable
    throw RuntimeException("Unreachable code has been reached! " +
        "Requested: '${key}@${timestamp}', floor: '${floorKey}', ceil: '${ceilKey}'.")
}

fun temporalGet(keyspace: String, key: String, timestamp: Long, dataProvider: GetDataProvider): GetResult<ByteArray> {
    require(keyspace.isNotEmpty()) { "Argument 'keyspace' must not be empty!" }
    require(key.isNotEmpty()) { "Argument 'key' must not be empty!" }
    require(timestamp >= 0) { "Argument 'timestamp' must not be negative!" }
    val (floorEntry, higherEntry) = dataProvider.get(key, timestamp)
    return temporalGet(keyspace, key, timestamp, floorEntry, higherEntry)
}


interface GetDataProvider {

    fun get(key: String, timestamp: Long): GetDataResult

    interface GetDataResult {

        val floorEntry: Pair<UnqualifiedTemporalKey, ByteArray>?

        val higherEntry: Pair<UnqualifiedTemporalKey, ByteArray>?

        operator fun component1(): Pair<UnqualifiedTemporalKey, ByteArray>? {
            return floorEntry
        }

        operator fun component2(): Pair<UnqualifiedTemporalKey, ByteArray>? {
            return higherEntry
        }

    }
}

fun ByteArray.zeroLengthToNull(): ByteArray? {
    if (this.size <= 0) {
        // treat zero-length arrays as NULL
        return null
    } else {
        return this
    }
}
