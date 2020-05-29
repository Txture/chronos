package org.chronos.chronodb.exodus.secondaryindex.stores

import com.google.common.annotations.VisibleForTesting
import jetbrains.exodus.ByteIterable
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.LongContainmentCondition
import org.chronos.chronodb.api.query.NumberCondition
import org.chronos.chronodb.exodus.kotlin.ext.parseAsLong
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.ASCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.DESCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.SCAN_UNTIL_END
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.STOP_AT_FIRST_MISMATCH
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.util.readLongsFromBytes
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentLongSearchSpecification
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification
import java.util.regex.Pattern

object SecondaryLongIndexStore : SecondaryIndexStore<Long, LongSearchSpecification>() {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    private const val LONG_BYTES = java.lang.Long.BYTES

    private val STORE_NAME_PATTERN = Pattern.compile("${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_LONG}(\\d+)/(.*)")

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @VisibleForTesting
    fun storeName(indexName: String, keyspace: String): String {
        return "${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_LONG}${keyspace.length}/$keyspace}${indexName}"
    }

    fun getIndexNameForStoreName(storeName: String): String? {
        val nameWithoutPrefix = if(storeName.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_LONG)){
            storeName.removePrefix(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_LONG)
        }else {
            return null
        }
        val matcher = Pattern.compile("\\d+").matcher(nameWithoutPrefix)
        val found = matcher.find()
        if(!found){
            return null
        }
        val lengthOfKeyspaceNameAsString = matcher.group()
        val lengthOfKeyspaceName = lengthOfKeyspaceNameAsString.toInt()
        return nameWithoutPrefix.substring(
                // remove the digits which measure the keyspace length
                lengthOfKeyspaceNameAsString.length
                        + 1 // also remove the '/' character
                        + lengthOfKeyspaceName // remove the keyspace
        )
    }

    private fun parseStoreName(storeName: String): Pair<String, String> {
        val matcher = STORE_NAME_PATTERN.matcher(storeName)
        if (!matcher.matches()) {
            throw IllegalArgumentException("Not a valid store name for secondary long index stores: ${storeName}")
        }
        val keyspaceNameLength = matcher.group(1).toInt()
        val keyspaceAndIndexName = matcher.group(2)
        val keyspace = keyspaceAndIndexName.substring(0, keyspaceNameLength)
        val indexName = keyspaceAndIndexName.substring(keyspaceNameLength, keyspaceAndIndexName.length)
        return Pair(keyspace, indexName)
    }

    override fun storeEntry(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Long, userKey: String, timestamps: ByteIterable) {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        tx.put(storeName, key, timestamps)
    }

    override fun loadValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Long, userKey: String): ByteIterable? {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        return tx.get(storeName, key)
    }

    override fun deleteValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: Long, userKey: String): Boolean {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        return tx.delete(storeName, key)
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

    @VisibleForTesting
    fun createSecondaryIndexKey(indexValue: Long, userKey: String): ByteArray {
        // serialize the index value
        val indexValueBytes = indexValue.toByteIterable().toByteArray()
        // serialize the user key
        val userKeyBytes = userKey.toByteIterable().toByteArray()
        // the final array has the following format:
        //
        // [longValue, 64bit][userKey]
        //
        // ... so we have to size it appropriately
        val array = ByteArray(LONG_BYTES + userKeyBytes.size)
        // copy over the index value bytes
        System.arraycopy(indexValueBytes, 0, array, 0, indexValueBytes.size)
        // copy over the user key bytes
        System.arraycopy(userKeyBytes, 0, array, indexValueBytes.size, userKeyBytes.size)
        return array
    }

    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteArray): SecondaryIndexKey<Long> {
        return this.parseSecondaryIndexKey(bytes.toByteIterable())
    }


    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteIterable): SecondaryIndexKey<Long> {
        // extract the index value
        val indexValueBytes = bytes.subIterable(0, LONG_BYTES)
        // extract the user key
        val userKeyBytes = bytes.subIterable(LONG_BYTES, bytes.length - LONG_BYTES)
        return SecondaryIndexKey(indexValueBytes, userKeyBytes, ByteIterable::parseAsLong)
    }

    override fun scan(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
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

    fun scan(tx: ExodusTransaction, searchSpec: ContainmentLongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        return when (searchSpec.condition) {
            LongContainmentCondition.WITHIN -> {
                val searchValues = searchSpec.searchValue
                when(searchValues.size){
                    0 -> ScanResult(emptyList())
                    in 1..3 -> {
                        // interpret small sets as connected OR queries; take the union of their scan results
                        val entries = searchValues.asSequence().flatMap { searchValue ->
                            val innerSearchSpec = LongSearchSpecification.create(
                                searchSpec.property,
                                Condition.EQUALS,
                                searchValue
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
            LongContainmentCondition.WITHOUT -> scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            else -> scanGeneric(tx, searchSpec, keyspace, timestamp, scanTimeMode)
        }
    }

    override fun allEntries(tx: ExodusTransaction, keyspace: String, propertyName: String, consumer: RawIndexEntryConsumer<Long>) {
        this.allEntries(tx, this.storeName(propertyName, keyspace), this::parseSecondaryIndexKey, consumer)
    }

    // =================================================================================================================
    // SCAN METHODS
    // =================================================================================================================

    private fun scanEquals(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = this.storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                scanStart = searchSpec.searchValue.toByteIterable(),
                direction = ASCENDING,
                scanStrategy = STOP_AT_FIRST_MISMATCH,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.ASCENDING))
    }

    private fun scanGreaterEqual(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = this.storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                scanStart = searchSpec.searchValue.toByteIterable(),
                direction = ASCENDING,
                scanStrategy = STOP_AT_FIRST_MISMATCH,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.ASCENDING))
    }

    private fun scanGreaterThan(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = this.storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                // note: we do NOT want the search value in the result set, so we add 1
                scanStart = (searchSpec.searchValue + 1).toByteIterable(),
                direction = ASCENDING,
                scanStrategy = STOP_AT_FIRST_MISMATCH,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.ASCENDING))
    }

    private fun scanLessEqual(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = this.storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                // as we are scanning DESCENDING, we start one higher (because the primary keys are appended)...
                scanStart = (searchSpec.searchValue + 1).toByteIterable(),
                // ... and skip over greater values.
                skip = { indexValue: Long -> indexValue > searchSpec.searchValue },
                direction = DESCENDING,
                scanStrategy = STOP_AT_FIRST_MISMATCH,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.DESCENDING))
    }

    private fun scanLessThan(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                // note: we do NOT subtract 1 from the search value here. When we scan DESCENDING and we pass in the
                // search value (without any appended primary key) as scan start position, then we are already BELOW all
                // entries in the index (as all index keys have the primary keys appended to them).
                scanStart = searchSpec.searchValue.toByteIterable(),
                direction = DESCENDING,
                scanStrategy = STOP_AT_FIRST_MISMATCH,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.DESCENDING))
    }

    private fun scanGeneric(tx: ExodusTransaction, searchSpec: LongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, LongSearchSpecification>(
                tx = tx,
                storeName = storeName(searchSpec.property, keyspace),
                searchSpec = searchSpec,
                timestamp = timestamp,
                scanStart = null,
                direction = ASCENDING,
                scanStrategy = SCAN_UNTIL_END,
                parseKey = this::parseSecondaryIndexKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.ASCENDING))
    }

    private fun scanGeneric(tx: ExodusTransaction, searchSpec: ContainmentLongSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<Long> {
        val scanConfiguration = IndexScanConfiguration<Long, ContainmentLongSearchSpecification>(
            tx = tx,
            storeName = storeName(searchSpec.property, keyspace),
            searchSpec = searchSpec,
            timestamp = timestamp,
            scanStart = null,
            direction = ASCENDING,
            scanStrategy = SCAN_UNTIL_END,
            parseKey = this::parseSecondaryIndexKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = scanConfiguration.performScan()
        return ScanResult(resultList, OrderedBy(searchSpec.property, Order.ASCENDING))
    }

}