package org.chronos.chronodb.exodus.secondaryindex

import jetbrains.exodus.ByteIterable
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.TextCompare
import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.query.searchspec.*
import org.chronos.chronodb.internal.impl.index.cursor.BasicIndexScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.RawIndexCursor
import kotlin.reflect.KClass

object ExodusChunkIndex {

    @Suppress("UNCHECKED_CAST")
    fun <T> scanForResults(tx: ExodusTransaction, timestamp: Long, keyspace: String, searchSpec: SearchSpecification<T, *>): ScanResult<T> {
        return scanInternal(tx, timestamp, keyspace, searchSpec, ScanTimeMode.SCAN_FOR_PERIOD_MATCHES)
    }

    fun <T> scanForTerminations(tx: ExodusTransaction, timestamp: Long, keyspace: String, searchSpec: SearchSpecification<T, *>): ScanResult<T> {
        return scanInternal(tx, timestamp, keyspace, searchSpec, ScanTimeMode.SCAN_FOR_TERMINATED_PERIODS)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> scanInternal(tx: ExodusTransaction, timestamp: Long, keyspace: String, searchSpec: SearchSpecification<T, *>, scanTimeMode: ScanTimeMode): ScanResult<T> {
        return when (searchSpec) {
            is DoubleSearchSpecification -> SecondaryDoubleIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            is LongSearchSpecification -> SecondaryLongIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            is StringSearchSpecification -> SecondaryStringIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            is ContainmentStringSearchSpecification -> SecondaryStringIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            is ContainmentLongSearchSpecification -> SecondaryLongIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            is ContainmentDoubleSearchSpecification -> SecondaryDoubleIndexStore.scan(tx, searchSpec, keyspace, timestamp, scanTimeMode) as ScanResult<T>
            else -> throw IllegalArgumentException("Unknown type of query (class: ${searchSpec.javaClass.name})!")
        }
    }

    fun applyModifications(tx: ExodusTransaction, modifications: ExodusIndexModifications, lowerBound: Long) {
        if (modifications.isEmpty) {
            return
        }
        val timestamp = modifications.changeTimestamp
        for (termination in modifications.terminations) {
            when (termination.value) {
                is String -> executeStringTermination(tx, termination, timestamp, lowerBound)
                is Short, is Int, is Long -> executeLongTermination(tx, termination, timestamp, lowerBound)
                is Float, is Double -> executeDoubleTermination(tx, termination, timestamp, lowerBound)
                else -> throw IllegalArgumentException("Cannot index value - it's type does not match any known indices! " +
                    "Value Class: ${termination.value.javaClass.name}, Value: '${termination.value}'")
            }
        }
        for (addition in modifications.additions) {
            when (addition.value) {
                is String -> executeStringAddition(tx, addition, timestamp)
                is Short, is Int, is Long -> executeLongAddition(tx, addition, timestamp)
                is Float, is Double -> executeDoubleAddition(tx, addition, timestamp)
                else -> throw IllegalArgumentException("Cannot index value - it's type does not match any known indices! " +
                    "Value Class: ${addition.value.javaClass.name}, Value: '${addition.value}'")
            }
        }
    }

    @Suppress("unchecked_cast")
    fun <V : Any> allEntries(tx: ExodusTransaction, keyspace: String, propertyName: String, type: KClass<V>, consumer: RawIndexEntryConsumer<V>) {
        when (type) {
            String::class -> SecondaryStringIndexStore.allEntries(tx, keyspace, propertyName, consumer as RawIndexEntryConsumer<String>)
            Long::class -> SecondaryLongIndexStore.allEntries(tx, keyspace, propertyName, consumer as RawIndexEntryConsumer<Long>)
            Double::class -> SecondaryDoubleIndexStore.allEntries(tx, keyspace, propertyName, consumer as RawIndexEntryConsumer<Double>)
            else -> throw IllegalArgumentException("Unknown index type: ${type.qualifiedName}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <V : Comparable<V>> createIndexScanCursor(
        tx: ExodusTransaction,
        keyspace: String,
        timestamp: Long,
        index: SecondaryIndex,
        order: Order,
        textCompare: TextCompare
    ): IndexScanCursor<V> {
        val rawCursor = this.createRawIndexCursor<V>(tx, keyspace, index, order, textCompare)
        return BasicIndexScanCursor(rawCursor, timestamp)
    }

    @Suppress("UNCHECKED_CAST")
    fun <V : Comparable<V>> createRawIndexCursor(
        tx: ExodusTransaction,
        keyspace: String,
        index: SecondaryIndex,
        order: Order,
        textCompare: TextCompare
    ): RawIndexCursor<V> {
        val (cursor, parseKey) = when (index.valueType) {
            String::class.java -> {
                val storeName = when (textCompare) {
                    TextCompare.STRICT -> SecondaryStringIndexStore.storeName(index.id, keyspace)
                    TextCompare.CASE_INSENSITIVE -> SecondaryStringIndexStore.storeNameCI(index.id, keyspace)
                }
                val parseKey = { k: ByteIterable -> SecondaryStringIndexStore.parseSecondaryIndexKey(k) as SecondaryIndexKey<V> }
                val cursor = tx.openCursorOn(storeName)
                Pair(cursor, parseKey)
            }
            Long::class.java -> {
                val storeName = SecondaryLongIndexStore.storeName(index.id, keyspace)
                val parseKey = { k: ByteIterable -> SecondaryLongIndexStore.parseSecondaryIndexKey(k) as SecondaryIndexKey<V> }
                val cursor = tx.openCursorOn(storeName)
                Pair(cursor, parseKey)
            }
            Double::class.java -> {
                val storeName = SecondaryDoubleIndexStore.storeName(index.id, keyspace)
                val parseKey = { k: ByteIterable -> SecondaryDoubleIndexStore.parseSecondaryIndexKey(k) as SecondaryIndexKey<V> }
                val cursor = tx.openCursorOn(storeName)
                Pair(cursor, parseKey)
            }
            else -> throw IllegalArgumentException("Unknown index type: ${index.valueType.name}")
        }
        return ExodusRawIndexCursor(cursor, order, parseKey)
    }

    // =================================================================================================================
    // PRIVATE HELPER METHODS
    // =================================================================================================================

    private fun executeStringAddition(tx: ExodusTransaction, addition: ExodusIndexEntryAddition, timestamp: Long) {
        SecondaryStringIndexStore.insert(
            tx = tx,
            indexId = addition.index.id,
            indexValue = addition.value as String,
            keyspace = addition.keyspace,
            userKey = addition.key,
            validFrom = timestamp
        )
    }

    private fun executeStringTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryStringIndexStore.terminateValidity(
            tx = tx,
            indexId = termination.index.id,
            indexValue = termination.value as String,
            keyspace = termination.keyspace,
            userKey = termination.key,
            timestamp = timestamp,
            lowerBound = lowerBound
        )
    }

    private fun executeLongAddition(tx: ExodusTransaction, addition: ExodusIndexEntryAddition, timestamp: Long) {
        SecondaryLongIndexStore.insert(
            tx = tx,
            indexId = addition.index.id,
            indexValue = (addition.value as Number).toLong(),
            keyspace = addition.keyspace,
            userKey = addition.key,
            validFrom = timestamp
        )
    }

    private fun executeLongTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryLongIndexStore.terminateValidity(
            tx = tx,
            indexId = termination.index.id,
            indexValue = (termination.value as Number).toLong(),
            keyspace = termination.keyspace,
            userKey = termination.key,
            timestamp = timestamp,
            lowerBound = lowerBound
        )
    }

    private fun executeDoubleAddition(tx: ExodusTransaction, addition: ExodusIndexEntryAddition, timestamp: Long) {
        SecondaryDoubleIndexStore.insert(
            tx = tx,
            indexId = addition.index.id,
            indexValue = (addition.value as Number).toDouble(),
            keyspace = addition.keyspace,
            userKey = addition.key,
            validFrom = timestamp
        )
    }

    private fun executeDoubleTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryDoubleIndexStore.terminateValidity(
            tx = tx,
            indexId = termination.index.id,
            indexValue = (termination.value as Double).toDouble(),
            keyspace = termination.keyspace,
            userKey = termination.key,
            timestamp = timestamp,
            lowerBound = lowerBound
        )
    }


}