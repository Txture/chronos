package org.chronos.chronodb.exodus.manager

import com.google.common.collect.Sets
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.ExodusDataMatrixUtil
import org.chronos.chronodb.exodus.TemporalExodusMatrix
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.chunk.RolloverProcessInfo
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.BranchInternal
import org.chronos.chronodb.internal.api.CommitMetadataStore
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.TemporalDataMatrix
import org.chronos.chronodb.internal.impl.MatrixUtils
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey

class ExodusTkvs : AbstractTemporalKeyValueStore {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    private val commitMetadataStore: CommitMetadataStore
    private var cachedNowTimestamp: Long
    private val owningDB: ExodusChronoDB
        get() = super.getOwningDB() as ExodusChronoDB

    private val walTokenCacheLock = Object()
    private var walTokenCache: WriteAheadLogToken? = null

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB, branch: BranchInternal) : super(owningDB, branch) {
        this.commitMetadataStore = ExodusCommitMetadataStore(owningDB.globalChunkManager, owningDB.serializationManager, branch)
        this.cachedNowTimestamp = -1L
        this.initializeBranch()
        this.initializeKeyspaceToMatrixMapFromDB()
        this.initializeWalTokenCache()
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun getNowInternal(): Long {
        this.withNonExclusiveLock {
            if (this.cachedNowTimestamp < 0) {
                val storedNowTimestamp = this.withGlobalReadOnlyTransaction { tx ->
                    val stored = tx.get(ChronoDBStoreLayout.STORE_NAME__BRANCH_TO_NOW, this.owningBranch.name)
                    stored?.parseAsLong().orIfNull(0L)
                }
                val bcm = this.owningDB.globalChunkManager.getOrCreateChunkManagerForBranch(this.owningBranch)
                val lastChunkValidFrom = bcm.headChunk.validPeriod.lowerBound
                this.cachedNowTimestamp = Math.max(storedNowTimestamp, lastChunkValidFrom)
            }
        }
        return this.cachedNowTimestamp
    }

    override fun setNow(timestamp: Long) {
        this.withBranchExclusiveLock {
            // invalidate cache
            this.cachedNowTimestamp = -1
            this.withGlobalReadWriteTransaction { tx ->
                tx.put(ChronoDBStoreLayout.STORE_NAME__BRANCH_TO_NOW, this.owningBranch.name, timestamp.toByteIterable())
                tx.commit()
            }
        }

    }

    override fun createMatrix(keyspace: String, timestamp: Long): TemporalDataMatrix {
        requireNonNegative(timestamp, "timestamp")
        val matrixTableName = MatrixUtils.generateRandomName()
        val branchName = this.owningBranch.name
        return this.withGlobalReadWriteTransaction { tx ->
            NavigationIndex.insert(tx, branchName, keyspace, matrixTableName, timestamp)
            val matrix = TemporalExodusMatrix(this.owningDB.globalChunkManager, branchName, matrixTableName, keyspace, timestamp)
            tx.commit()
            this.keyspaceToMatrix[keyspace] = matrix
            matrix
        }
    }

    private fun initializeWalTokenCache() {
        synchronized(this.walTokenCacheLock) {
            val serializationManager = this.owningDB.serializationManager
            this.walTokenCache = this.withGlobalReadOnlyTransaction { tx ->
                tx.get(ChronoDBStoreLayout.STORE_NAME__BRANCH_TO_WAL, this.owningBranch.name)
                    .mapSingle { serializationManager.deserialize(it.toByteArray()) as WriteAheadLogToken }
            }
        }
    }

    override fun getWriteAheadLogTokenIfExists(): WriteAheadLogToken? {
        synchronized(this.walTokenCacheLock) {
            return this.walTokenCache
        }
    }

    override fun getCommitMetadataStore(): CommitMetadataStore {
        return this.commitMetadataStore
    }

    override fun performWriteAheadLog(token: WriteAheadLogToken) {
        synchronized(this.walTokenCacheLock) {
            this.withGlobalReadWriteTransaction { tx ->
                val serialForm = this.owningDB.serializationManager.serialize(token).toByteIterable()
                tx.put(ChronoDBStoreLayout.STORE_NAME__BRANCH_TO_WAL, this.owningBranch.name, serialForm)
                tx.commit()
            }
            this.walTokenCache = token
        }
    }

    override fun clearWriteAheadLogToken() {
        synchronized(this.walTokenCacheLock) {
            this.withGlobalReadWriteTransaction { tx ->
                tx.delete(ChronoDBStoreLayout.STORE_NAME__BRANCH_TO_WAL, this.owningBranch.name)
                tx.commit()
            }
            this.walTokenCache = null
        }
    }

    fun performRollover(updateIndices: Boolean) {
        val now = this.now
        this.owningDB.lockExclusive().use {
            // record the rollover timestamp
            var timestamp = System.currentTimeMillis()
            if (now == timestamp) {
                // don't roll over exactly at the commit timestamp
                timestamp = now + 1
            }
            val chunkManager = this.owningDB.globalChunkManager.getOrCreateChunkManagerForBranch(this.owningBranch)
            chunkManager.performRollover(timestamp, this::transferHeadRevisionIntoNewChunk)
            // clear our "now" timestamp cache (creation of new chunk changes timestamp calculation)
            this.cachedNowTimestamp = -1
            if (updateIndices) {
                // make sure that we have an index on the head revision
                this.owningDB.indexManager.reindexHeadRevision(this.owningBranch.name)
            } else {
                // mark all indices as dirty
                this.owningDB.indexManager.markAllIndicesAsDirty()
            }
            // clear the branch head statistics for this branch
            this.owningDB.statisticsManager.clearBranchHeadStatistics(this.owningBranch.name)
        }
    }

    override fun datebackCleanup(branch: String, earliestTouchedTimestamp: Long) {
        require(earliestTouchedTimestamp >= 0) { "Precondition violation - argument 'earliestTouchedTimestamp' must not be negative!" }
        super.datebackCleanup(branch, earliestTouchedTimestamp)
        // if there are no changed keys, we are done
        if (earliestTouchedTimestamp == Long.MAX_VALUE) {
            return
        }
        // get the chunk manager for the affected branch
        val bcm = this.owningDB.globalChunkManager.getChunkManagerForBranch(branch)
        // get the chunks in ascending order
        val chunksForPeriod = bcm.getChunksForPeriod(Period.createOpenEndedRange(earliestTouchedTimestamp));
        if (chunksForPeriod.isEmpty()) {
            // this should never happen...
            return
        }
        // in the following algorithm, we reconstruct the rollover boundaries. This is done as follows:
        //
        //    +-----+      +-----+     +-----+     +-----+
        //    |  1  | ---> |  2  |---> |  3  |---> |  4  |
        //    +-----+      +-----+     +-----+     +-----+
        //
        // Let's assume that chunk 1 is the earliest one which has been modified, and chunk 4 is HEAD.
        //
        // The algorithm has the following schema:
        //
        // - Clear the rollover commit in chunk 2 (because we cannot detect removed keys otherwise)
        // - Read the latest values for each key from chunk 1
        // - Write those values into the rollover "base version" in chunk 2
        //
        // - Clear the rollover commit in chunk 3 (because we cannot detect removed keys otherwise)
        // - Read the latest values for each key from chunk 2
        // - Write those values into the rollover "base version" in chunk 3
        //
        // - Clear the rollover commit in chunk 4 (because we cannot detect removed keys otherwise)
        // - Read the latest values for each key from chunk 3
        // - Write those values into the rollover "base version" in chunk 4

        for (readFromChunkIndex in 0 until chunksForPeriod.size - 1) {
            val writeToChunkIndex = readFromChunkIndex + 1
            val readChunk = chunksForPeriod.get(readFromChunkIndex)
            val writeChunk = chunksForPeriod.get(writeToChunkIndex)

            this.owningDB.globalChunkManager.openReadWriteTransactionOn(writeChunk).use { writeTx ->
                this.owningDB.globalChunkManager.openReadOnlyTransactionOn(readChunk).use { readTx ->
                    // get the names of the stores which represent the matrices. Note that we do NOT include inverse store names here,
                    // because inverse stores are handled separately.
                    val matrixStoreNames = this.keyspaceToMatrix.values.asSequence().cast(TemporalExodusMatrix::class).map { it.storeName }.toList()

                    for (matrixStoreName in matrixStoreNames) {
                        if (!readTx.storeExists(matrixStoreName)) {
                            // the matrix did not exist at that point in time yet, skip
                            continue
                        }
                        // clear the rollover timestamp in the write chunk
                        clearEntriesAtTimestamp(writeTx, matrixStoreName, writeChunk.validPeriod.lowerBound)
                        writeTx.flush()

                        copyAllLatestEntriesFromReadTxIntoWriteTx(readTx, writeTx, matrixStoreName, writeChunk.validPeriod.lowerBound)
                        writeTx.flush()
                    }
                }
                writeTx.commit()
            }
        }

    }

    /**
     * Takes the *last* entry for each user key in the given matrix store in the read transaction, and copies it into the store of the same name using the given write transaction (unless the last entry is a deletion).
     * The write will occur with the given destination timestamp.
     *
     * Example:
     * ```
     * +---------------+
     * | KEY  | VALUE  |
     * +------+--------+
     * | a@1  | bytes1 |
     * | a@25 | bytes2 |
     * | b@30 | bytes3 |
     * | b@40 | bytes4 |
     * | c@13 | bytes5 |
     * +------+--------+
     * ```
     *
     * This would copy:
     * - a@25 into a@destination with value bytes2
     * - b@40 into b@destination with value bytes4
     * - c@13 into b@destination with values bytes5
     *
     * Please note that if the last entry happens to be a deletion (bytes.length == 0), it will **not** be copied!
     *
     * @param readTx The transaction to use for reading.
     * @param writeTx The transaction to write to.
     * @param matrixStoreName The name of the store which represents the matrix in question.
     * @param destinationTimestamp The timestamp to write to.
     */
    private fun copyAllLatestEntriesFromReadTxIntoWriteTx(readTx: ExodusTransaction, writeTx: ExodusTransaction, matrixStoreName: String, destinationTimestamp: Long) {
        readTx.withCursorOn(matrixStoreName) { cursor ->
            var previousKey: UnqualifiedTemporalKey? = null
            var previousValue: ByteIterable? = null
            while (cursor.next) {
                val currentKey = cursor.key.parseAsUnqualifiedTemporalKey()
                // check if we reached a new (user) key
                if (previousKey != null && currentKey.key != previousKey.key) {
                    // we have reached the next user key. The previous key
                    // was the LAST modification on this user key in this chunk,
                    // so we have to transfer it into the next chunk

                    // (except if the value happens to be the empty byte iterable, which
                    // indicates a deletion; deletions do not need to be copied
                    // so we skip them here)
                    if (!previousValue!!.isEmpty()) {
                        val targetQKey = UnqualifiedTemporalKey.create(previousKey.key, destinationTimestamp)
                        this.transferMatrixEntryIntoNextChunk(targetQKey, previousValue, writeTx, matrixStoreName)
                    }
                }
                previousKey = currentKey
                previousValue = cursor.value
            }
            // the last entry of the store must ALWAYS be copied into the next chunk
            if (previousKey != null && previousValue != null && !previousValue.isEmpty()) {
                val targetQKey = UnqualifiedTemporalKey.create(previousKey.key, destinationTimestamp)
                this.transferMatrixEntryIntoNextChunk(targetQKey, previousValue, writeTx, matrixStoreName)
            }
        }
    }

    /**
     * Clears the entries in the given matrix store (and inverse store) which reside *exactly* at the given timestamp.
     *
     * @param tx The transaction to use for writing.
     * @param matrixStoreName The name of the matrix store.
     * @param timestamp The timestamp to clear.
     */
    private fun clearEntriesAtTimestamp(tx: ExodusTransaction, matrixStoreName: String, timestamp: Long) {
        tx.withCursorOn(matrixStoreName) { cursor ->
            while (cursor.next) {
                if (cursor.key.parseAsUnqualifiedTemporalKey().timestamp <= timestamp) {
                    cursor.deleteCurrent()
                }
            }
        }
        tx.withCursorOn(matrixStoreName + TemporalExodusMatrix.INVERSE_STORE_NAME_SUFFIX) { cursor ->
            while (cursor.next) {
                if (cursor.key.parseAsInverseUnqualifiedTemporalKey().timestamp <= timestamp) {
                    cursor.deleteCurrent()
                }
            }
        }
    }

    private fun transferMatrixEntryIntoNextChunk(qKey: UnqualifiedTemporalKey, value: ByteIterable, writeTx: ExodusTransaction, matrixStoreName: String) {
        // write into the store...
        writeTx.put(matrixStoreName, qKey.toByteIterable(), value)
        // ... and into the inverse store
        writeTx.put(matrixStoreName + TemporalExodusMatrix.INVERSE_STORE_NAME_SUFFIX, qKey.inverse().toByteIterable(), BooleanBinding.booleanToEntry(!value.isEmpty()))
    }

    private fun transferHeadRevisionIntoNewChunk(rolloverProcessInfo: RolloverProcessInfo) {
        val newHeadChunk = rolloverProcessInfo.newHeadChunk
        val rolloverTimestamp = rolloverProcessInfo.rolloverTimestamp
        val branchName = this.owningBranch.name
        val chunkManager = this.owningDB.globalChunkManager
        val keyspaceNameToMapName = getKeyspaceNameToMapName(branchName, rolloverProcessInfo)
        val tx = this.owningDB.tx(branchName)
        val entries = Sets.newHashSet<UnqualifiedTemporalEntry>()
        val maxBatchSize = this.owningDB.configuration.rolloverBatchSize
        for (keyspace in tx.keyspaces()) {
            val mapName = keyspaceNameToMapName[keyspace] ?: throw IllegalStateException("Keyspace '${keyspace}' is unknown!")
            val keySet = tx.keySet(keyspace)
            for (key in keySet) {
                val value: ByteArray = tx.getBinary(keyspace, key) ?: continue
                val utKey = UnqualifiedTemporalKey(key, rolloverTimestamp)
                val utEntry = UnqualifiedTemporalEntry(utKey, value)
                entries.add(utEntry)
                if (entries.size >= maxBatchSize) {
                    // flush the data onto disk
                    chunkManager.openReadWriteTransactionOn(newHeadChunk).use { exodusTx ->
                        ExodusDataMatrixUtil.insertEntries(exodusTx, mapName, entries)
                        entries.clear()
                        exodusTx.commit()
                    }
                }
            }
            if (entries.isNotEmpty()) {
                // perform a last flush
                chunkManager.openReadWriteTransactionOn(newHeadChunk).use { exodusTx ->
                    ExodusDataMatrixUtil.insertEntries(exodusTx, mapName, entries)
                    entries.clear()
                    exodusTx.commit()
                }
            }
        }
    }

    private fun getKeyspaceNameToMapName(branchName: String, rolloverProcessInfo: RolloverProcessInfo): Map<String, String> {
        val keyspaceMetadata = this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { rootDbTx ->
            return@use NavigationIndex.getKeyspaceMetadata(rootDbTx, branchName)
        }
        val keyspaceNameToMapName = keyspaceMetadata.associate { it.keyspaceName to it.matrixTableName }.toMutableMap()
        val tx = this.owningDB.tx(branchName)
        if (!rolloverProcessInfo.oldHeadChunk.isDeltaChunk){
            return keyspaceNameToMapName
        }
        // if we are a delta chunk, we may need to create additional keyspaces which haven't been used in the branch yet. See if there are any.
        val keyspacesWithoutMetadata = tx.keyspaces().asSequence()
            .filter { it !in keyspaceNameToMapName }
            .toSet()

        if (keyspacesWithoutMetadata.isEmpty()){
            // every keyspace has some metadata, nothing to do.
            return keyspaceNameToMapName
        }
        // there are some keyspaces which have not been used in the delta chunk, create the metadata for them.
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { rootDbTx ->
            for (keyspaceWithoutMetadata in keyspacesWithoutMetadata) {
                val newMetadata = NavigationIndex.insert(
                    tx = rootDbTx,
                    branchName = branchName,
                    keyspaceName = keyspaceWithoutMetadata,
                    matrixName = MatrixUtils.generateRandomName(),
                    timestamp = this.owningBranch.branchingTimestamp
                )

                keyspaceNameToMapName[keyspaceWithoutMetadata] = newMetadata.matrixTableName
            }
        }
        return keyspaceNameToMapName
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private fun initializeBranch() {
        this.withGlobalReadWriteTransaction { tx ->
            val branchName = this.owningBranch.name
            if (NavigationIndex.existsBranch(tx, branchName)) {
                log.trace { "Branch '$branchName' already exists." }
            } else {
                val keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME
                val tableName = MatrixUtils.generateRandomName()
                log.trace { "Creating branch: [$branchName, $keyspaceName, $tableName]" }
                NavigationIndex.insert(tx, branchName, keyspaceName, tableName, 0L)
                tx.commit()
            }
        }
    }

    private fun initializeKeyspaceToMatrixMapFromDB() {
        this.withGlobalReadWriteTransaction { tx ->
            val branchName = this.owningBranch.name
            val allKeyspaceMetadata = NavigationIndex.getKeyspaceMetadata(tx, branchName)
            for (keyspaceMetadata in allKeyspaceMetadata) {
                val keyspace = keyspaceMetadata.keyspaceName
                val matrixTableName = keyspaceMetadata.matrixTableName
                val timestamp = keyspaceMetadata.creationTimestamp
                val matrix = TemporalExodusMatrix(
                    owningDB.globalChunkManager,
                    branchName, matrixTableName, keyspace, timestamp
                )
                this.keyspaceToMatrix[keyspace] = matrix
                log.trace { "Registering keyspace '$keyspace' matrix in branch '$branchName': $matrixTableName" }
            }
            tx.commit()
        }
    }

    private fun <T> withNonExclusiveLock(action: () -> T): T {
        this.lockNonExclusive().use {
            return action()
        }
    }

    private fun <T> withExclusiveLock(action: () -> T): T {
        this.lockExclusive().use {
            return action()
        }
    }

    private fun <T> withBranchExclusiveLock(action: () -> T): T {
        this.lockBranchExclusive().use {
            return action()
        }
    }

    private fun <T> withGlobalReadOnlyTransaction(action: (ExodusTransaction) -> T): T {
        return this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use(action)
    }

    private fun <T> withGlobalReadWriteTransaction(action: (ExodusTransaction) -> T): T {
        return this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use(action)
    }


}
