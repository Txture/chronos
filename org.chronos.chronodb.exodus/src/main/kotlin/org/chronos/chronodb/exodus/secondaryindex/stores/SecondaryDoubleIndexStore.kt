package org.chronos.chronodb.exodus.secondaryindex.stores

import com.google.common.annotations.VisibleForTesting
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.SignedDoubleBinding
import jetbrains.exodus.bindings.StringBinding
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.DoubleContainmentCondition
import org.chronos.chronodb.api.query.NumberCondition
import org.chronos.chronodb.exodus.kotlin.ext.parseAsDouble
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.ASCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.DESCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.SCAN_UNTIL_END
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.STOP_AT_FIRST_MISMATCH
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification
import java.util.regex.Pattern
import kotlin.math.absoluteValue

object SecondaryDoubleIndexStore : SecondaryIndexStore<Double, DoubleSearchSpecification>() {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    private const val DOUBLE_BYTES = java.lang.Double.BYTES

    private val STORE_NAME_PATTERN = Pattern.compile("${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_DOUBLE}(\\d+)\\/(.*)")


    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    fun getIndexIdForStoreName(storeName: String): String? {
        return this.parseStoreNameOrNull(storeName)?.second
    }

    private fun parseStoreName(storeName: String): Pair<String, String> {
        return this.parseStoreNameOrNull(storeName)
            ?: throw IllegalArgumentException("Not a valid store name for secondary long index stores: ${storeName}")
    }

    private fun parseStoreNameOrNull(storeName: String): Pair<String, String>? {
        val matcher = STORE_NAME_PATTERN.matcher(storeName)
        if (!matcher.matches()) {
            return null
        }
        val keyspaceNameLength = matcher.group(1).toInt()
        val keyspaceAndIndexId = matcher.group(2)
        val keyspace = keyspaceAndIndexId.substring(0, keyspaceNameLength)
        val indexName = keyspaceAndIndexId.substring(keyspaceNameLength, keyspaceAndIndexId.length)
        return Pair(keyspace, indexName)
    }

    override fun storeEntry(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Double, userKey: String, timestamps: ByteIterable) {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        tx.put(storeName, key, timestamps)
    }

    override fun loadValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Double, userKey: String): ByteIterable? {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        return tx.get(storeName, key)
    }

    override fun deleteValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Double, userKey: String): Boolean {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        return tx.delete(storeName, key)
    }

    override fun scan(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        return when (searchSpec.condition) {
            !is NumberCondition -> throw IllegalStateException("Condition ${searchSpec.condition} is not applicable to DOUBLE indices!")
            NumberCondition.EQUALS -> scanEquals(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            NumberCondition.GREATER_EQUAL -> scanGreaterEqual(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            NumberCondition.GREATER_THAN -> scanGreaterThan(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            NumberCondition.LESS_EQUAL -> scanLessEqual(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            NumberCondition.LESS_THAN -> scanLessThan(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            else -> scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
        }
    }

    fun scan(tx: ExodusTransaction, searchSpec: ContainmentDoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        return when (searchSpec.condition) {
            DoubleContainmentCondition.WITHIN -> {
                val searchValues = searchSpec.searchValue
                when (searchValues.size) {
                    0 -> ScanResult(emptyList())
                    in 1..3 -> {
                        // interpret small sets as connected OR queries; take the union of their scan results
                        val entries = searchValues.asSequence().flatMap { searchValue ->
                            val innerSearchSpec = DoubleSearchSpecification.create(
                                searchSpec.index,
                                Condition.EQUALS,
                                searchValue,
                                searchSpec.equalityTolerance
                            )
                            return@flatMap this.scan(tx, innerSearchSpec, keyspace, timestamp, scanTimeMode).entries.asSequence()
                        }.distinct().toList()
                        // Note: we have no ordering on those results due to the set union; if we require an ordering, we would
                        // have to establish it here.
                        ScanResult(entries)
                    }
                    else -> this.scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
                }
            }
            DoubleContainmentCondition.WITHOUT -> this.scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            else -> scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
        }
    }


    override fun rollback(tx: ExodusTransaction, indexName: String, timestamp: Long, keys: Set<QualifiedKey>?) {
        if (keys == null) {
            // roll back all keys
            tx.getAllStoreNames()
                .asSequence()
                .filter { it.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_LONG) }
                .filter { parseStoreName(it).second == indexName }
                .forEach { storeName ->
                    this.rollbackInternal(tx, storeName, timestamp, this::parseSecondaryIndexKey)
                }
        } else {
            // roll back one keyspace at a time
            keys.groupBy { it.keyspace }.forEach { (keyspace, qKeys) ->
                val keysToRollBack = qKeys.asSequence().map { it.key }.toSet()
                val storeName = this.storeName(indexName, keyspace)
                this.rollbackInternal(tx, storeName, timestamp, this::parseSecondaryIndexKey, keysToRollBack)
            }
        }
    }

    override fun allEntries(tx: ExodusTransaction, keyspace: String, propertyName: String, consumer: RawIndexEntryConsumer<Double>) {
        this.allEntries(tx, this.storeName(propertyName, keyspace), this::parseSecondaryIndexKey, consumer)
    }


    // =================================================================================================================
    // SCAN METHODS
    // =================================================================================================================

    private fun scanEquals(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        require(searchSpec.searchValue.isFinite()) { "Precondition violation - search spec contains a match value which is Infinity or NaN!" }
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            scanStart = (searchSpec.searchValue - searchSpec.equalityTolerance.absoluteValue).toByteIterable(),
            direction = ASCENDING,
            scanStrategy = STOP_AT_FIRST_MISMATCH,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.ASCENDING))
    }

    private fun scanGreaterEqual(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        require(searchSpec.searchValue.isFinite()) { "Precondition violation - search spec contains a match value which is Infinity or NaN!" }
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            // start a LITTLE bit lower than the search specification states...
            scanStart = (searchSpec.searchValue - 0.001).toByteIterable(),
            // ... and while going forward in the index, skip the values which are strictly smaller
            skip = { indexedValue: Double -> indexedValue < searchSpec.searchValue },
            direction = ASCENDING,
            scanStrategy = STOP_AT_FIRST_MISMATCH,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.ASCENDING))
    }

    private fun scanGreaterThan(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        require(searchSpec.searchValue.isFinite()) { "Precondition violation - search spec contains a match value which is Infinity or NaN!" }
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            scanStart = searchSpec.searchValue.toByteIterable(),
            // skip the first couple of entries where the indexed value is exactly equal to the search value
            skip = { indexedValue: Double -> indexedValue == searchSpec.searchValue },
            direction = ASCENDING,
            scanStrategy = STOP_AT_FIRST_MISMATCH,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.ASCENDING))
    }

    private fun scanLessEqual(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        require(searchSpec.searchValue.isFinite()) { "Precondition violation - search spec contains a match value which is Infinity or NaN!" }
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            // start a LITTLE bit higher than the search specification states...
            scanStart = (searchSpec.searchValue + 0.001).toByteIterable(),
            // ... and while going backwards the index, skip the values which are strictly greater
            skip = { indexedValue: Double -> indexedValue > searchSpec.searchValue },
            direction = DESCENDING,
            scanStrategy = STOP_AT_FIRST_MISMATCH,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.DESCENDING))
    }

    private fun scanLessThan(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        require(searchSpec.searchValue.isFinite()) { "Precondition violation - search spec contains a match value which is Infinity or NaN!" }
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            scanStart = searchSpec.searchValue.toByteIterable(),
            // skip the first couple of entries where the indexed value is exactly equal to the search value
            skip = { indexedValue: Double -> indexedValue == searchSpec.searchValue },
            direction = DESCENDING,
            scanStrategy = STOP_AT_FIRST_MISMATCH,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.DESCENDING))
    }

    private fun scanGeneric(tx: ExodusTransaction, searchSpec: DoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        val scanConfiguration = IndexScanConfiguration<Double, DoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            scanStart = null,
            direction = ASCENDING,
            scanStrategy = SCAN_UNTIL_END,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.ASCENDING))
    }

    private fun scanGeneric(tx: ExodusTransaction, searchSpec: ContainmentDoubleSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Double> {
        val scanConfiguration = IndexScanConfiguration<Double, ContainmentDoubleSearchSpecification>(
            tx = tx,
            searchSpec = searchSpec,
            storeName = storeName(searchSpec.index.id, keyspace),
            timestamp = timestamp,
            scanStart = null,
            direction = ASCENDING,
            scanStrategy = SCAN_UNTIL_END,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.index.name, Order.ASCENDING))
    }

    // =================================================================================================================
    // INTERNAL METHODS
    // =================================================================================================================

    @VisibleForTesting
    fun createSecondaryIndexKey(indexValue: Double, userKey: String): ByteArray {
        // serialize the index value
        val indexValueBytes = SignedDoubleBinding.doubleToEntry(indexValue).toByteArray()
        // serialize the user key
        val userKeyBytes = StringBinding.stringToEntry(userKey).toByteArray()
        // the final array has the following format:
        //
        // [doubleValue, 64bit][userKey]
        //
        // ... so we have to size it appropriately
        val array = ByteArray(DOUBLE_BYTES + userKeyBytes.size)
        // copy over the index value bytes
        System.arraycopy(indexValueBytes, 0, array, 0, indexValueBytes.size)
        // copy over the user key bytes
        System.arraycopy(userKeyBytes, 0, array, indexValueBytes.size, userKeyBytes.size)
        return array
    }

    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteIterable): SecondaryIndexKey<Double> {
        // extract the index value
        val indexValueBytes = bytes.subIterable(0, DOUBLE_BYTES)
        // extract the user key
        val userKeyBytes = bytes.subIterable(DOUBLE_BYTES, bytes.length - DOUBLE_BYTES)
        return SecondaryIndexKey(indexValueBytes, userKeyBytes, ByteIterable::parseAsDouble)
    }

    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteArray): SecondaryIndexKey<Double> {
        return this.parseSecondaryIndexKey(bytes.toByteIterable())
    }

    @VisibleForTesting
    fun storeName(indexId: String, keyspace: String): String {
        return "${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_DOUBLE}${keyspace.length}/$keyspace}${indexId}"
    }

}