package org.chronos.chronodb.exodus.secondaryindex

import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.query.searchspec.*
import kotlin.reflect.KClass

object ExodusChunkIndex {

    @Suppress("UNCHECKED_CAST")
    fun <T> scanForResults(tx: ExodusTransaction, timestamp: Long, keyspace: String, searchSpec: SearchSpecification<T, *>): ScanResult<T> {
        return scanInternal(tx, timestamp, keyspace, searchSpec, ScanTimeMode.SCAN_FOR_PERIOD_MATCHES)
    }

    fun <T> scanForTerminations(tx: ExodusTransaction, timestamp: Long, keyspace: String, searchSpec: SearchSpecification<T, *>): ScanResult<T> {
        return scanInternal(tx, timestamp, keyspace, searchSpec, ScanTimeMode.SCAN_FOR_TERMINATED_PERIODS)
    }

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
        if(modifications.isEmpty){
            return
        }
        val timestamp = modifications.changeTimestamp
        modifications.terminations.forEach { termination ->
            when (termination.value) {
                is String -> executeStringTermination(tx, termination, timestamp, lowerBound)
                is Short, is Int, is Long -> executeLongTermination(tx, termination, timestamp, lowerBound)
                is Float, is Double -> executeDoubleTermination(tx, termination, timestamp, lowerBound)
                else -> throw IllegalArgumentException("Cannot index value - it's type does not match any known indices! " +
                        "Value Class: ${termination.value.javaClass.name}, Value: '${termination.value}'")
            }
        }
        modifications.additions.forEach { addition ->
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

    // =================================================================================================================
    // PRIVATE HELPER METHODS
    // =================================================================================================================

    private fun executeStringAddition(tx: ExodusTransaction, addition: ExodusIndexEntryAddition, timestamp: Long) {
        SecondaryStringIndexStore.insert(
                tx = tx,
                indexName = addition.index,
                indexValue = addition.value as String,
                keyspace = addition.keyspace,
                userKey = addition.key,
                validFrom = timestamp
        )
    }

    private fun executeStringTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryStringIndexStore.terminateValidity(
                tx = tx,
                indexName = termination.index,
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
                indexName = addition.index,
                indexValue = (addition.value as Number).toLong(),
                keyspace = addition.keyspace,
                userKey = addition.key,
                validFrom = timestamp
        )
    }

    private fun executeLongTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryLongIndexStore.terminateValidity(
                tx = tx,
                indexName = termination.index,
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
                indexName = addition.index,
                indexValue = (addition.value as Number).toDouble(),
                keyspace = addition.keyspace,
                userKey = addition.key,
                validFrom = timestamp
        )
    }

    private fun executeDoubleTermination(tx: ExodusTransaction, termination: ExodusIndexEntryTermination, timestamp: Long, lowerBound: Long) {
        SecondaryDoubleIndexStore.terminateValidity(
                tx = tx,
                indexName = termination.index,
                indexValue = (termination.value as Double).toDouble(),
                keyspace = termination.keyspace,
                userKey = termination.key,
                timestamp = timestamp,
                lowerBound = lowerBound
        )
    }

}