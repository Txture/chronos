package org.chronos.chronodb.exodus.secondaryindex.stores

import com.google.common.annotations.VisibleForTesting
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.StringBinding
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.StringCondition
import org.chronos.chronodb.api.query.StringContainmentCondition
import org.chronos.chronodb.exodus.kotlin.ext.parseAsString
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.ASCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.SCAN_UNTIL_END
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy.STOP_AT_FIRST_MISMATCH
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.util.readInt
import org.chronos.chronodb.exodus.util.writeInt
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentStringSearchSpecification
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification
import org.chronos.chronodb.internal.impl.query.TextMatchMode
import org.chronos.chronodb.internal.impl.query.TextMatchMode.CASE_INSENSITIVE
import org.chronos.chronodb.internal.impl.query.TextMatchMode.STRICT
import java.util.*
import java.util.regex.Pattern

object SecondaryStringIndexStore : SecondaryIndexStore<String, StringSearchSpecification>() {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    private val STORE_NAME_PATTERN = Pattern.compile("${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING}(\\d+)\\/(.*)")
    private val STORE_NAME_PATTERN_CI = Pattern.compile("${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING_CASE_INSENSITIVE}(\\d+)\\/(.*)")

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun storeEntry(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: String, userKey: String, timestamps: ByteIterable) {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val keyCI = createSecondaryIndexKeyCI(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        val storeNameCI = storeNameCI(indexName, keyspace)
        tx.put(storeName, key, timestamps)
        tx.put(storeNameCI, keyCI, timestamps)
    }

    override fun loadValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: String, userKey: String): ByteIterable? {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        return tx.get(storeName, key)
    }

    override fun deleteValue(tx: ExodusTransaction, indexName: String, keyspace: String, indexValue: String, userKey: String): Boolean {
        val key = createSecondaryIndexKey(indexValue, userKey).toByteIterable()
        val keyCI = createSecondaryIndexKeyCI(indexValue, userKey).toByteIterable()
        val storeName = storeName(indexName, keyspace)
        val storeNameCI = storeNameCI(indexName, keyspace)
        val deletedFromRegularIndex = tx.delete(storeName, key)
        val deletedFromCIIndex = tx.delete(storeNameCI, keyCI)
        return deletedFromRegularIndex || deletedFromCIIndex
    }

    override fun scan(tx: ExodusTransaction, searchSpec: StringSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<String> {
        return when (searchSpec.condition) {
            !is StringCondition -> throw IllegalStateException("Condition ${searchSpec.condition} is not applicable to STRING indices!")
            StringCondition.STARTS_WITH -> scanAscendingFromSearchValue(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            StringCondition.EQUALS -> scanAscendingFromSearchValue(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            else -> scanAscendingFullTable(tx, searchSpec, keyspace, timestamp, scanTimeMode)
        }
    }

    fun scan(tx: ExodusTransaction, searchSpec: ContainmentStringSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<String> {
        return when (searchSpec.condition) {
            StringContainmentCondition.WITHIN -> {
                val searchValues = searchSpec.searchValue
                when(searchValues.size){
                    0 -> ScanResult(emptyList())
                    in 1..3 -> {
                        // interpret small sets as connected OR queries; take the union of their scan results
                        val entries = searchValues.asSequence().flatMap { searchValue ->
                            val innerSearchSpec = StringSearchSpecification.create(
                                searchSpec.index,
                                Condition.EQUALS,
                                searchSpec.matchMode,
                                searchValue
                            )
                            return@flatMap this.scan(tx, innerSearchSpec, keyspace, timestamp, scanTimeMode).entries.asSequence()
                        }.distinct().toList()
                        // Note: we have no ordering on those results due to the set union; if we require an ordering, we would
                        // have to establish it here.
                        ScanResult(entries)
                    }
                    else -> this.scanAscendingFullTable(tx, searchSpec, keyspace, timestamp, scanTimeMode)
                }
            }
            StringContainmentCondition.WITHOUT -> scanAscendingFullTable(tx, searchSpec, keyspace, timestamp, scanTimeMode)
            else -> scanAscendingFullTable(tx, searchSpec, keyspace, timestamp, scanTimeMode)
        }
    }

    override fun rollback(tx: ExodusTransaction, indexId: String, timestamp: Long, keys: Set<QualifiedKey>?) {
        if (keys == null) {
            val allStoreNames = tx.getAllStoreNames()
            // roll back all keys
            allStoreNames
                    .asSequence()
                    .filter { it.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING) }
                    .filter { this.parseStoreName(it).second == indexId }
                    .forEach { storeName ->
                        this.rollbackInternal(tx, storeName, timestamp, this::parseSecondaryIndexKey)
                    }
            // don't forget about the case-insensitive index...
            allStoreNames
                    .asSequence()
                    .filter { it.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING_CASE_INSENSITIVE) }
                    .filter { this.parseStoreName(it).second == indexId }
                    .forEach { storeName ->
                        this.rollbackInternal(tx, storeName, timestamp, this::parseSecondaryIndexKeyCI)
                    }
        } else {
            // roll back one keyspace at a time (regular and CI indices)
            keys.groupBy { it.keyspace }.forEach { (keyspace, qKeys) ->
                val keysToRollBack = qKeys.asSequence().map { it.key }.toSet()
                val storeName = this.storeName(indexId, keyspace)
                this.rollbackInternal(tx, storeName, timestamp, this::parseSecondaryIndexKey, keysToRollBack)
                val storeNameCI = this.storeNameCI(indexId, keyspace)
                this.rollbackInternal(tx, storeNameCI, timestamp, this::parseSecondaryIndexKeyCI, keysToRollBack)
            }
        }
    }

    // =================================================================================================================
    // SCANNING METHODS
    // =================================================================================================================

    /**
     * Scans the index table in ascending fashion, starting at the entry specified in the search spec, and stopping at the first mismatch.
     *
     * @param tx The transaction to operate on.
     * @param searchSpec The search spec to evaluate.
     * @param keyspace The keyspace to search in.
     * @param timestamp The timestamp to evaluate the search results for. Must not be negative.
     * @param scanTimeMode Specifies which condition to apply to the time periods of each result.
     */
    private fun scanAscendingFromSearchValue(tx: ExodusTransaction, searchSpec: StringSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<String> {
        val scanStart = when (searchSpec.matchMode) {
            STRICT -> searchSpec.searchValue.toByteIterable()
            CASE_INSENSITIVE -> searchSpec.searchValue.toLowerCase(Locale.ENGLISH).toByteIterable()
            null -> throw IllegalArgumentException("Text match mode must not be NULL!")
        }
        return scanAscendingInternal(tx, searchSpec, keyspace, timestamp, scanStart, STOP_AT_FIRST_MISMATCH, scanTimeMode)
    }

    /**
     * Scans the entire index table in ascending fashion, from very first to very last entry.
     *
     * @param tx The transaction to operate on.
     * @param searchSpec The search spec to evaluate.
     * @param keyspace The keyspace to search in.
     * @param timestamp The timestamp to evaluate the search results for. Must not be negative.
     * @param scanTimeMode Specifies which condition to apply to the time periods of each result.
     */
    private fun scanAscendingFullTable(tx: ExodusTransaction, searchSpec: StringSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<String> {
        return scanAscendingInternal(tx, searchSpec, keyspace, timestamp, null, SCAN_UNTIL_END, scanTimeMode)
    }

    /**
     * Scans the entire index table in ascending fashion, from very first to very last entry.
     *
     * @param tx The transaction to operate on.
     * @param searchSpec The search spec to evaluate.
     * @param keyspace The keyspace to search in.
     * @param timestamp The timestamp to evaluate the search results for. Must not be negative.
     * @param scanTimeMode Specifies which condition to apply to the time periods of each result.
     */
    private fun scanAscendingFullTable(tx: ExodusTransaction, searchSpec: ContainmentStringSearchSpecification, keyspace: String, timestamp: Long, scanTimeMode: ScanTimeMode): ScanResult<String> {
        return scanAscendingInternal(tx, searchSpec, keyspace, timestamp, SCAN_UNTIL_END, scanTimeMode)
    }

    private fun scanAscendingInternal(tx: ExodusTransaction, searchSpec: ContainmentStringSearchSpecification, keyspace: String, timestamp: Long, scanStrategy: ScanStrategy, scanTimeMode: ScanTimeMode): ScanResult<String> {
        val storeName: String
        val parseKey: (ByteIterable) -> SecondaryIndexKey<String>
        when (searchSpec.matchMode) {
            STRICT -> {
                storeName = this.storeName(searchSpec.index.id, keyspace)
                parseKey = this::parseSecondaryIndexKey
            }
            CASE_INSENSITIVE -> {
                storeName = this.storeNameCI(searchSpec.index.id, keyspace)
                parseKey = this::parseSecondaryIndexKeyCI
            }
            null -> throw IllegalArgumentException("Text match mode must not be NULL!")
        }
        val scanConfiguration = IndexScanConfiguration(
            tx = tx,
            searchSpec = searchSpec,
            timestamp = timestamp,
            scanStart = null,
            direction = ASCENDING,
            scanStrategy = scanStrategy,
            storeName = storeName,
            parseKey = parseKey,
            scanTimeMode = scanTimeMode
        )
        val resultList = this.dedupIfCaseInsensitive(searchSpec.matchMode, scanConfiguration.performScan())
        val orderedBy: OrderedBy? = inferResultOrdering(scanConfiguration.direction, searchSpec.index.name, searchSpec.matchMode)
        return ScanResult(resultList, orderedBy)
    }

    private fun scanAscendingInternal(tx: ExodusTransaction, searchSpec: StringSearchSpecification, keyspace: String, timestamp: Long, scanStart: ByteIterable?, scanStrategy: ScanStrategy, scanTimeMode: ScanTimeMode): ScanResult<String> {
        val storeName: String
        val parseKey: (ByteIterable) -> SecondaryIndexKey<String>
        when (searchSpec.matchMode) {
            STRICT -> {
                storeName = this.storeName(searchSpec.index.id, keyspace)
                parseKey = this::parseSecondaryIndexKey
            }
            CASE_INSENSITIVE -> {
                storeName = this.storeNameCI(searchSpec.index.id, keyspace)
                parseKey = this::parseSecondaryIndexKeyCI
            }
            null -> throw IllegalArgumentException("Text match mode must not be NULL!")
        }
        val scanConfiguration = IndexScanConfiguration(
                tx = tx,
                searchSpec = searchSpec,
                timestamp = timestamp,
                scanStart = scanStart,
                direction = ASCENDING,
                scanStrategy = scanStrategy,
                storeName = storeName,
                parseKey = parseKey,
                scanTimeMode = scanTimeMode
        )
        val resultList = this.dedupIfCaseInsensitive(searchSpec.matchMode, scanConfiguration.performScan())
        val orderedBy: OrderedBy? = inferResultOrdering(scanConfiguration.direction, searchSpec.index.name, searchSpec.matchMode)
        return ScanResult(resultList, orderedBy)
    }

    override fun allEntries(tx: ExodusTransaction, keyspace: String, propertyName: String, consumer: RawIndexEntryConsumer<String>) {
        // start with the case sensitive store
        this.allEntries(tx, this.storeName(propertyName, keyspace), this::parseSecondaryIndexKey, consumer)
        // then report the CI store
        this.allEntries(tx, this.storeNameCI(propertyName, keyspace), this::parseSecondaryIndexKeyCI, consumer)
    }


    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    @VisibleForTesting
    fun storeName(indexId: String, keyspace: String): String {
        return "${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING}${keyspace.length}/${keyspace}${indexId}"
    }

    @VisibleForTesting
    fun storeNameCI(indexId: String, keyspace: String): String {
        return "${ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING_CASE_INSENSITIVE}${keyspace.length}/${keyspace}${indexId}"
    }

    fun getIndexIdForStoreName(storeName: String): String? {
        return this.parseStoreNameOrNull(storeName)?.second
    }

    private fun parseStoreName(storeName: String): Pair<String, String> {
        return this.parseStoreNameOrNull(storeName)
            ?: throw IllegalArgumentException("Not a valid store name for secondary string index stores: ${storeName}")
    }

    private fun parseStoreNameOrNull(storeName: String): Pair<String, String>? {
        val pattern = when {
            storeName.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING) -> STORE_NAME_PATTERN
            storeName.startsWith(ChronoDBStoreLayout.STORE_NAME_PREFIX__SECONDARY_INDEX_STRING_CASE_INSENSITIVE) -> STORE_NAME_PATTERN_CI
            else -> return null
        }
        val matcher = pattern.matcher(storeName)
        if (!matcher.matches()) {
            return null
        }
        val keyspaceNameLength = matcher.group(1).toInt()
        val keyspaceAndIndexId = matcher.group(2)
        val keyspace = keyspaceAndIndexId.substring(0, keyspaceNameLength)
        val indexId = keyspaceAndIndexId.substring(keyspaceNameLength, keyspaceAndIndexId.length)
        return Pair(keyspace, indexId)
    }

    @VisibleForTesting
    fun createSecondaryIndexKey(indexValue: String, userKey: String): ByteArray {
        // serialize the index value
        val indexValueBytes = StringBinding.stringToEntry(indexValue).toByteArray()
        // serialize the user key
        val userKeyBytes = StringBinding.stringToEntry(userKey).toByteArray()
        // the final array has the following format:
        //
        // [indexValue][userKey][length of index value (in bytes) as int]
        //
        // ... so we have to size it appropriately
        val array = ByteArray(indexValueBytes.size + userKeyBytes.size + Integer.BYTES)
        // copy over the index value bytes
        System.arraycopy(indexValueBytes, 0, array, 0, indexValueBytes.size)
        // copy over the user key bytes
        System.arraycopy(userKeyBytes, 0, array, indexValueBytes.size, userKeyBytes.size)
        // write the length of the index value as an int
        writeInt(
                buffer = array,
                value = indexValueBytes.size,
                offset = indexValueBytes.size + userKeyBytes.size
        )
        return array
    }

    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteArray): SecondaryIndexKey<String> {
        return this.parseSecondaryIndexKey(bytes.toByteIterable())
    }

    @VisibleForTesting
    fun parseSecondaryIndexKey(bytes: ByteIterable): SecondaryIndexKey<String> {
        val bytesUnsafe = bytes.bytesUnsafe
        val indexValueLength = readInt(bytesUnsafe, bytes.length - Integer.BYTES)
        // extract the index value
        val indexValueBytes = bytes.subIterable(0, indexValueLength)
        // extract the user key
        val userKeyBytes = bytes.subIterable(indexValueLength, bytes.length - Integer.BYTES - indexValueLength)
        return SecondaryIndexKey(indexValueBytes, userKeyBytes, ByteIterable::parseAsString)
    }

    @VisibleForTesting
    fun createSecondaryIndexKeyCI(indexValue: String, userKey: String): ByteArray {
        val indexValueBytesLower = indexValue.toLowerCase(Locale.ENGLISH).toByteIterable().toByteArray()
        val indexValueBytesOriginal = indexValue.toByteIterable().toByteArray()
        val userKeyBytes = userKey.toByteIterable().toByteArray()
        // the final array has the following format:
        //
        // [indexValueLowercase][indexValue][userKey][length of lowercase index value (in bytes) as int][length of original index value (in bytes) as int]
        //
        // ... so we have to size it appropriately
        val array = ByteArray(indexValueBytesLower.size + indexValueBytesOriginal.size + userKeyBytes.size + Integer.BYTES * 2)
        var position = 0
        System.arraycopy(indexValueBytesLower, 0, array, position, indexValueBytesLower.size)
        position += indexValueBytesLower.size
        System.arraycopy(indexValueBytesOriginal, 0, array, position, indexValueBytesOriginal.size)
        position += indexValueBytesOriginal.size
        System.arraycopy(userKeyBytes, 0, array, position, userKeyBytes.size)
        position += userKeyBytes.size
        writeInt(
                buffer = array,
                value = indexValueBytesLower.size,
                offset = position
        )
        position += Integer.BYTES
        writeInt(
                buffer = array,
                value = indexValueBytesOriginal.size,
                offset = position
        )
        return array
    }

    @VisibleForTesting
    fun parseSecondaryIndexKeyCI(bytes: ByteArray): SecondaryIndexKey<String> {
        return this.parseSecondaryIndexKeyCI(bytes.toByteIterable())
    }


    @VisibleForTesting
    fun parseSecondaryIndexKeyCI(bytes: ByteIterable): SecondaryIndexKey<String> {
        val rawBytes = bytes.bytesUnsafe
        val originalIndexValueLength = readInt(rawBytes, bytes.length - Integer.BYTES)
        val lowercaseIndexValueLength = readInt(rawBytes, bytes.length - Integer.BYTES * 2)
        // extract the index value (lowercase)
        val indexValue = bytes.subIterable(0, lowercaseIndexValueLength)
        // extract the user key
        val userKey = bytes.subIterable(lowercaseIndexValueLength + originalIndexValueLength, bytes.length - Integer.BYTES * 2 - originalIndexValueLength - lowercaseIndexValueLength)
        return SecondaryIndexKey(indexValue, userKey, ByteIterable::parseAsString)
    }

    private fun inferResultOrdering(scanDirection: ScanDirection, property: String, textMatchMode: TextMatchMode): OrderedBy? {
        return when (textMatchMode) {
            // when matching against the STRICT (case sensitive) index, we always know the ordering
            TextMatchMode.STRICT -> OrderedBy(property, when (scanDirection) {
                ScanDirection.ASCENDING -> Order.ASCENDING; ScanDirection.DESCENDING -> Order.DESCENDING
            })
            // when matching against the case-insensitive index, the ordering is unknown
            // (actually it is the order of the lower-cased index values, but that information is not very useful, so we discard it here)
            TextMatchMode.CASE_INSENSITIVE -> null
        }
    }

    private fun dedupIfCaseInsensitive(textMatchMode: TextMatchMode, list: List<ScanResultEntry<String>>): List<ScanResultEntry<String>> {
        return when (textMatchMode) {
            CASE_INSENSITIVE -> list.distinct()
            else -> list
        }
    }


}