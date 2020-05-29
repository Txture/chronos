package org.chronos.chronodb.exodus.secondaryindex

import com.google.common.collect.*
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.index.IndexManagerBackend
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.chronos.chronodb.internal.util.MultiMapUtil
import org.chronos.common.logging.ChronoLogger
import java.nio.file.Files
import kotlin.reflect.KClass

class ExodusIndexManagerBackend : IndexManagerBackend {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    companion object {
        private const val REINDEX_FLUSH_INTERVAL = 25_000
    }


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private val owningDB: ExodusChronoDB

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB) {
        this.owningDB = owningDB
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun loadIndexersFromPersistence(): SetMultimap<String, Indexer<*>> {
        return this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            this.loadIndexersMap(tx)
        }
    }

    override fun persistIndexers(indexNameToIndexers: SetMultimap<String, Indexer<*>>) {
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            this.persistIndexersMap(tx, indexNameToIndexers)
            tx.commit()
        }
    }

    override fun persistIndexer(indexName: String, indexer: Indexer<*>) {
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val map = this.loadIndexersMap(tx)
            map.put(indexName, indexer)
            this.persistIndexersMap(tx, map)
            tx.commit()
        }
        this.deleteChunkIndices(setOf(indexName))
    }

    override fun deleteIndexAndIndexers(indexName: String?) {
        // first, delete the indexers
        val indexersMap = this.loadIndexersFromPersistence()
        indexersMap.removeAll(indexName)
        this.persistIndexers(indexersMap)
        // we do not immediately cleanup current and old chunks; specific index is not written/updated from now on
    }

    override fun deleteAllIndicesAndIndexers() {
        // first, delete the indexers
        val indexersMap = HashMultimap.create<String, Indexer<*>>()
        this.persistIndexers(indexersMap)
        // delete all index chunks
        this.deleteAllChunkIndices()
    }

    @Suppress("UNCHECKED_CAST")
    override fun loadIndexStates(): Map<String, Boolean> {
        return this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            this.getIndexDirtyFlagsSerialForm(tx)
                    .mapSingle { this.deserialize(it) as Map<String, Boolean> }
                    .orIfNull(mapOf())
        }
    }

    override fun persistIndexDirtyStates(indexNameToDirtyFlag: Map<String, Boolean>) {
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val serializedForm = this.owningDB.serializationManager.serialize(Maps.newHashMap(indexNameToDirtyFlag))
            this.saveIndexDirtyFlags(tx, serializedForm)
            tx.commit()
        }
    }

    override fun rollback(branches: Set<String>, timestamp: Long) {
        requireNonNegative(timestamp, "timestamp")
        this.rollbackInternal(branches, timestamp, null)
    }

    override fun rollback(branches: Set<String>, timestamp: Long, keys: Set<QualifiedKey>) {
        requireNonNegative(timestamp, "timestamp")
        this.rollbackInternal(branches, timestamp, keys)
    }

    private fun rollbackInternal(branches: Set<String>, timestamp: Long, keys: Set<QualifiedKey>?) {
        requireNonNegative(timestamp, "timestamp")
        val gcm = this.owningDB.globalChunkManager
        val indexersByIndexName = this.owningDB.indexManager.indexersByIndexName
        for (branch in branches) {
            val headChunk = gcm.getChunkManagerForBranch(branch).headChunk
            check(headChunk.validPeriod.contains(timestamp)) {
                "Cannot roll back branch ${branch} to timestamp ${timestamp} - " +
                        "it is not within the validity period of the HEAD chunk (${headChunk.validPeriod})!"
            }
            gcm.openReadWriteTransactionOn(headChunk.indexDirectory).use { tx ->
                val rolledBackIndices = mutableSetOf<Pair<String, KClass<*>>>()
                indexersByIndexName.forEach { (indexName, indexers) ->
                    indexers.forEach { indexer ->
                        when (indexer) {
                            is StringIndexer -> {
                                if (!rolledBackIndices.contains(Pair(indexName, String::class))) {
                                    SecondaryStringIndexStore.rollback(tx, indexName, timestamp, keys)
                                    rolledBackIndices.add(indexName to String::class)
                                }
                            }
                            is DoubleIndexer -> {
                                if (!rolledBackIndices.contains(Pair(indexName, Double::class))) {
                                    SecondaryDoubleIndexStore.rollback(tx, indexName, timestamp, keys)
                                    rolledBackIndices.add(indexName to Double::class)
                                }
                            }
                            is LongIndexer -> {
                                if (!rolledBackIndices.contains(Pair(indexName, Long::class))) {
                                    SecondaryLongIndexStore.rollback(tx, indexName, timestamp, keys)
                                    rolledBackIndices.add(Pair(indexName, Long::class))
                                }
                            }
                            else -> throw IllegalArgumentException("Unknown indexer type: ${indexer.javaClass.name}")
                        }
                        // flush after each indexer to write changes to disk (but not commit them yet)
                        tx.flush()
                    }
                }
                tx.commit()
            }
        }
    }

    fun rebuildIndexOnAllChunks() {
        val gcm = this.owningDB.globalChunkManager
        // iterate over all branches
        for (branch in this.owningDB.branchManager.branches) {
            val branchChunkManager = gcm.getOrCreateChunkManagerForBranch(branch)
            // iterate over all chunks
            val chunks = branchChunkManager.getChunksForPeriod(Period.createOpenEndedRange(0))
            for (chunk in chunks) {
                this.rebuildIndexForChunk(chunk)
            }
        }
    }

    fun applyModifications(indexModifications: ExodusIndexModifications) {
        val gcm = this.owningDB.globalChunkManager
        indexModifications.groupByBranch().forEach { (branchName, modifications) ->
            val chunk = gcm.getChunkManagerForBranch(branchName).getChunkForTimestamp(modifications.changeTimestamp)
            if (chunk == null) {
                throw IllegalStateException("Cannot apply index modifications - there is no chunk for this time period! " +
                        "Change timestamp: ${modifications.changeTimestamp}, Branch: '${branchName}'")
            }
            if (!chunk.indexDirectory.exists()) {
                Files.createDirectory(chunk.indexDirectory.toPath())
            }
            val branchingTimestamp = this.owningDB.branchManager.getBranch(branchName).branchingTimestamp
            val lowerBound = Math.max(branchingTimestamp, chunk.validPeriod.lowerBound)
            gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
                ExodusChunkIndex.applyModifications(tx, modifications, lowerBound)
                tx.commit()
            }
        }
    }

    fun <T> performSearch(timestamp: Long, branch: Branch, keyspace: String, searchSpec: SearchSpecification<T, *>): Set<String> {
        requireNonNegative(timestamp, "timestamp")
        val gcm = this.owningDB.globalChunkManager
        val branches = Lists.newArrayList(branch.originsRecursive)
        // always add the branch we are actually interested in to the end of the list
        branches.add(branch)

        // we will later require information when a branch was "branched out" into the child branch we are interested
        // in, so we build this map now. It is a mapping from a branch to the timestamp when the child branch
        // was created. It is the same info as branch#getBranchingTimestamp(), just attached to the parent (not the
        // child).
        //
        // Example:
        // We have: {"B", origin: "A", timestamp: 1234}, {"A", origin: "master", timestamp: 123}, {"master"}
        // We need: {"master" branchedIntoA_At: 123} {"A" branchedIntoB_At: 1234} {"B"}
        val branchOutTimestamps = Maps.newHashMap<Branch, Long>()
        for (i in branches.indices) {
            val b = branches[i]
            if (b == branch) {
                // the request branch has no "branch out" timestamp
                continue
            }
            val childBranch = branches[i + 1]
            // the root branch was "branched out" to the child branch at this timestamp
            branchOutTimestamps[b] = childBranch.branchingTimestamp
        }
        // prepare the result map
        val resultMap = ArrayListMultimap.create<String, ScanResultEntry<T>>()
        // now, iterate over the list and repeat the matching algorithm for every branch:
        // 1) Collect the matches from the origin branch
        // 2) Remove from these matches all of those which were deleted in our current branch
        // 3) Add the matches local to our current branch
        for (currentBranch in branches) {
            val branchName = currentBranch.name
            // Important note: if the branch we are currently dealing with is NOT the branch that we are
            // querying, but a PARENT branch instead, we must use the MINIMUM of
            // (branching timestamp; request timestamp),
            // because the branch might have changes that are AFTER the branching timestamp but BEFORE
            // the request timestamp, and we don't want to see those in the result.
            val scanTimestamp = when {
                currentBranch != branch -> Math.min(timestamp, branchOutTimestamps[currentBranch]!!)
                else -> timestamp
            }
            if (scanTimestamp < currentBranch.branchingTimestamp) {
                // The query has already been answered by our parent branch(es).
                // There is no need to question our secondary index, or that of our child branches,
                // because the result will not change anymore. This is due to the scan timestamp
                // being EARLIER than our branching timestamp.
                break
            }
            // check if we have a non-empty result set (in that case, we have to respect branch-local deletions)
            val chunk = gcm.getChunkManagerForBranch(branchName).getChunkForTimestamp(scanTimestamp)
            if (chunk == null) {
                // there is no chunk here... we have no information about this branch/timestamp combination.
                continue
            }
            if (!resultMap.isEmpty) {
                gcm.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                    val terminations = ExodusChunkIndex.scanForTerminations(tx, scanTimestamp, keyspace, searchSpec)
                    terminations.forEach { termination ->
                        val resultEntries = Lists.newArrayList(resultMap[termination.primaryKey])
                        resultEntries.forEach { resultEntry ->
                            if (resultEntry.indexedValue == termination.indexedValue) {
                                resultMap.remove(termination.primaryKey, resultEntry)
                            }
                        }
                    }
                }
            }
            if (branches.size == 1) {
                // only master branch, no need to do all the magic...
                val result = gcm.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                    ExodusChunkIndex.scanForResults(tx, scanTimestamp, keyspace, searchSpec)
                }
                val set = Sets.newHashSetWithExpectedSize<String>(result.size)
                for (entry in result) {
                    set.add(entry.primaryKey)
                }
                return set
            }
            // find the branch-local matches of the given branch.
            gcm.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                val additions = ExodusChunkIndex.scanForResults(tx, scanTimestamp, keyspace, searchSpec)
                additions.forEach { addition ->
                    resultMap.put(addition.primaryKey, addition)
                }
            }
        }
        return resultMap.keySet()
    }

    fun <T : Any> allEntries(branch: String, keyspace: String, propertyName: String, type: KClass<T>, consumer: IndexEntryConsumer<T>) {
        val bcm = this.owningDB.globalChunkManager.getChunkManagerForBranch(branch)
        val branchChunks = bcm.getChunksForPeriod(Period.eternal())
        for (chunk in branchChunks) {
            this.owningDB.globalChunkManager.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                ExodusChunkIndex.allEntries(tx, keyspace, propertyName, type) { storeName, primaryKey, indexedValue, validityPeriods ->
                    consumer(IndexEntry(branch, chunk.sequenceNumber, storeName, primaryKey, indexedValue, validityPeriods))
                }
            }
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun rebuildIndexForChunk(chunk: ChronoChunk) {
        val gcm = this.owningDB.globalChunkManager
        val indexers = MultiMapUtil.copyToMultimap(this.owningDB.indexManager.indexersByIndexName)
        val isDeltaChunk = this.isBranchDeltaChunk(chunk)

        gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { indexTx ->
            // first, truncate all data in this environment (in each store)
            indexTx.getAllStoreNames().forEach {
                indexTx.removeStore(it)
            }
            // note: it is imperative that we COMMIT here and do not continue using
            // the same transaction. The reason is that Exodus chokes on the situation
            // where a transaction first removes a store, and then re-creates it. This
            // causes strange artifacts and undefined behaviour. We therefore use a
            // dedicated transaction to delete the store, and another one to fill the
            // new (potentially equally named) store.
            indexTx.commit()
        }
        gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { indexTx ->
            // then, rebuild the index
            val branchName = chunk.branchName
            val chunkPeriod = chunk.validPeriod
            val allKeyspaceMetadata = gcm.openReadOnlyTransactionOnGlobalEnvironment().use { gTx ->
                NavigationIndex.getKeyspaceMetadata(gTx, branchName)
            }
            // iterate over all keyspaces
            for (keyspaceMetadata in allKeyspaceMetadata) {
                // check if the keyspace exists in our period
                val creationTimestamp = keyspaceMetadata.creationTimestamp
                val keyspaceExistencePeriod = Period.createOpenEndedRange(creationTimestamp)
                if (!chunkPeriod.overlaps(keyspaceExistencePeriod)) {
                    // the keyspace was created after the period we are interested in; skip it
                    continue
                }
                // extract the information required to get access to the underlying store
                val keyspaceName = keyspaceMetadata.keyspaceName
                val matrixName = keyspaceMetadata.matrixTableName

                // access the chunk data and index it.
                gcm.openReadOnlyTransactionOn(chunk).use { chunkTx ->
                    // iterate over all entries
                    chunkTx.withCursorOn(matrixName) { cursor ->
                        performReindexing(cursor, branchName, keyspaceName, isDeltaChunk, indexers, indexTx)
                    }
                }
            }
            indexTx.commit()
        }
    }

    private fun performReindexing(cursor: Cursor, branchName: String, keyspaceName: String, isDeltaChunk: Boolean, indexers: SetMultimap<String, Indexer<*>>?, indexTx: ExodusTransaction) {
        var previousKey: String? = null
        var previousValue: Any? = null
        var previousIndexValues: SetMultimap<String, Any>? = null
        var batchSize = 0
        // Important note: due to the way the primary index keys are
        // constructed, the following iteration order is ALWAYS employed:
        // - First, by user key ascending,
        // - then by timestamp ascending
        // Therefore, overrides of the same key always directly come after
        // one another. We make heavy use of this fact in the loop below.
        while (cursor.next) {
            val tKey = cursor.key.parseAsUnqualifiedTemporalKey()
            val value = deserialize(cursor.value)
            val indexModifications = ExodusIndexModifications(tKey.timestamp)
            if (value == null) {
                // deletion
                // Terminate all index entries of the previous key/value pair (if any)
                if (tKey.key == previousKey && previousIndexValues != null) {
                    terminateAll(branchName, keyspaceName, tKey.key, previousIndexValues, indexModifications)
                } else {
                    if (isDeltaChunk) {
                        // this is a deletion of an entry that doesn't belong to our
                        // branch -> we need to fetch it from the origin branch to
                        // calculate the index entries to terminate.
                        val previousObject: Any? = this.owningDB.tx(branchName, tKey.timestamp - 1).get(keyspaceName, tKey.key)
                        if (previousObject != null) { // just a safeguard; NULL can't really happen.
                            val indexValues = IndexingUtils.getIndexedValuesForObject(indexers, previousObject)
                            terminateAll(branchName, keyspaceName, tKey.key, indexValues, indexModifications)
                        } else {
                            ChronoLogger.logWarning("Detected unexpected case during secondary indexing. " +
                                    "The entry [${branchName}].[${keyspaceName}].[${tKey.key}]@${tKey.timestamp} is a deletion, " +
                                    "but a temporal GET to the predecessor produced NULL as well. Skipping this entry.")
                        }
                    } else {
                        // this is not a delta-chunk and the first entry on this key is a deletion? Weird.
                        ChronoLogger.logWarning("Detected unexpected case during secondary indexing. " +
                                "The entry [${branchName}].[${keyspaceName}].[${tKey.key}]@${tKey.timestamp} is a deletion, " +
                                "but it has no preceding insert. Skipping this entry.")
                    }
                }
                previousIndexValues = null
            } else {
                // insertion or update
                val indexValues = IndexingUtils.getIndexedValuesForObject(indexers, value)
                val indexingDiff = when {
                    tKey.key == previousKey && previousIndexValues != null -> {
                        // we have a direct update to our predecessor entry
                        IndexingUtils.calculateDiff(indexers, previousValue, value)
                    }
                    else -> {
                        // this is the first insert of this key in this chunk. If we are dealing
                        // with a delta chunk, we need to consider the predecessor entry in the
                        // origin chunk too.
                        val previousObject: Any? = when {
                            // the entry may override an entry of the origin branch. Fetch the original value (if any)
                            // Compiler note: it is *crucial* that we state the "Any?" type parameter in this "get" call because of a bug
                            // in the kotlin compiler. See: https://youtrack.jetbrains.com/issue/KT-29629
                            isDeltaChunk -> this.owningDB.tx(branchName, Math.max(0L, tKey.timestamp - 1)).get<Any?>(keyspaceName, tKey.key)
                            // this is not a delta chunk, there is no previous object we need to concern ourselves with.
                            else -> null
                        }
                        // we have a predecessor version (which may be NULL), calculate its index values
                        IndexingUtils.calculateDiff(indexers, previousObject, value)
                    }
                }
                // transform the diff into index modifications
                applyIndexingDiffToModifications(indexingDiff, indexModifications, branchName, keyspaceName, tKey)
                previousIndexValues = indexValues
            }
            val branchingTimestamp = this.owningDB.branchManager.getBranch(branchName).branchingTimestamp
            // apply the index modifications
            ExodusChunkIndex.applyModifications(indexTx, indexModifications, branchingTimestamp)
            // remember the results of this iteration (for use in the next iteration)
            previousKey = tKey.key
            previousValue = value
            batchSize++
            if (batchSize >= REINDEX_FLUSH_INTERVAL) {
                // flush our changes to disk (but do NOT commit yet!)
                indexTx.flush()
                batchSize = 0
            }
        }
    }

    private fun applyIndexingDiffToModifications(indexingDiff: IndexValueDiff, indexModifications: ExodusIndexModifications, branchName: String, keyspaceName: String, tKey: UnqualifiedTemporalKey) {
        indexingDiff.changedIndices.forEach { changedIndex ->
            indexingDiff.getAdditions(changedIndex).forEach { addedValue ->
                indexModifications.addEntryAddition(
                        ExodusIndexEntryAddition(
                                branchName,
                                changedIndex,
                                keyspaceName,
                                tKey.key,
                                addedValue
                        )
                )
            }
            indexingDiff.getRemovals(changedIndex).forEach { removedValue ->
                indexModifications.addEntryTermination(
                        ExodusIndexEntryTermination(
                                branchName,
                                changedIndex,
                                keyspaceName,
                                tKey.key,
                                removedValue
                        )
                )
            }
        }
    }

    private fun terminateAll(branchName: String, keyspaceName: String, key: String, previousIndexValues: SetMultimap<String, Any>, indexModifications: ExodusIndexModifications) {
        for (indexEntry in previousIndexValues.entries()) {
            indexModifications.addEntryTermination(
                    ExodusIndexEntryTermination(
                            branchName,
                            indexEntry.key,
                            keyspaceName,
                            key,
                            indexEntry.value
                    )
            )
        }
    }


    private fun deleteChunkIndices(indices: Set<String>) {
        val gcm = this.owningDB.globalChunkManager
        this.owningDB.branchManager.branchNames.forEach { branch ->
            val bcm = gcm.getChunkManagerForBranch(branch)
            bcm.getChunksForPeriod(Period.eternal()).forEach { chunk ->
                gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
                    tx.getAllStoreNames().forEach { storeName ->
                        val stringIndexName = SecondaryStringIndexStore.getIndexNameForStoreName(storeName)
                        val longIndexName = SecondaryLongIndexStore.getIndexNameForStoreName(storeName)
                        val doubleIndexName = SecondaryDoubleIndexStore.getIndexNameForStoreName(storeName)
                        if (stringIndexName != null && stringIndexName in indices) {
                            tx.removeStore(storeName)
                        } else {
                            if (longIndexName != null && longIndexName in indices) {
                                tx.removeStore(storeName)
                            } else if (doubleIndexName != null && doubleIndexName in indices) {
                                tx.removeStore(storeName)
                            }
                        }
                    }
                    tx.commit()
                }
            }
        }
    }

    private fun deleteAllChunkIndices() {
        val gcm = this.owningDB.globalChunkManager
        this.owningDB.branchManager.branchNames.forEach { branch ->
            val bcm = gcm.getChunkManagerForBranch(branch)
            bcm.getChunksForPeriod(Period.eternal()).forEach { chunk ->
                gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
                    tx.getAllStoreNames().forEach { storeName ->
                        tx.removeStore(storeName)
                    }
                    tx.commit()
                }
            }
        }

    }

    /**
     * Checks if the given chunk is the first in the chunk series for a non-master branch.
     *
     * Those chunks contain the delta to their origin branch at the branching timestamp. After the first rollover, they will contain the full information.
     *
     * @param chunk The chunk to check.
     * @return `true` if the given chunk is a delta chunk, i.e. the first chunk after a branching operation, otherwise `false`.
     */
    private fun isBranchDeltaChunk(chunk: ChronoChunk): Boolean {
        val validPeriod = chunk.validPeriod
        val branchName = chunk.branchName
        val branch = this.owningDB.branchManager.getBranch(branchName)
        if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER == branchName) {
            // the master branch has no delta chunks
            return false
        }
        // if our validity period contains the branching timestamp, then the chunk is the first in the series
        // (and therefore a delta chunk).
        return validPeriod.contains(branch.branchingTimestamp)
    }

    private fun deserialize(bytes: ByteIterable): Any? {
        val array = bytes.toByteArray()
        if (array.isEmpty()) {
            return null
        } else {
            return this.deserialize(array)
        }
    }

    private fun deserialize(bytes: ByteArray): Any? {
        return this.owningDB.serializationManager.deserialize(bytes)
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadIndexersMap(tx: ExodusTransaction): SetMultimap<String, Indexer<*>> {
        val map: Map<String, Set<Indexer<*>>> = this.getIndexersSerialForm(tx).mapSingle { serializedForm ->
            this.deserialize(serializedForm) as Map<String, Set<Indexer<*>>>?
        } ?: return HashMultimap.create()
        // we need to convert our internal map representation back into its multimap form
        return MultiMapUtil.copyToMultimap(map)
    }

    private fun persistIndexersMap(tx: ExodusTransaction, indexNameToIndexers: SetMultimap<String, Indexer<*>>) {
        // Kryo doesn't like to convert the SetMultimap class directly, so we transform
        // it into a regular hash map with sets as values.
        val persistentMap = MultiMapUtil.copyToMap(indexNameToIndexers)
        // first, serialize the indexers map to a binary format
        val serialForm = this.owningDB.serializationManager.serialize(persistentMap)
        // store the binary format in the database
        this.saveIndexers(tx, serialForm)
    }

    private fun getIndexersSerialForm(tx: ExodusTransaction): ByteArray? {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        return tx.get(indexName, key).mapSingle { it.toByteArray() }
    }

    private fun saveIndexers(tx: ExodusTransaction, serialForm: ByteArray) {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        tx.put(indexName, key, serialForm.toByteIterable())
    }

    private fun getIndexDirtyFlagsSerialForm(tx: ExodusTransaction): ByteArray? {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        return tx.get(indexName, key).mapSingle { it.toByteArray() }
    }

    private fun saveIndexDirtyFlags(tx: ExodusTransaction, serialForm: ByteArray) {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        tx.put(indexName, key, serialForm.toByteIterable())
    }

    fun rebuildIndexOnHeadChunk(branchName: String) {
        val branch = this.owningDB.branchManager.getBranch(branchName)
        val branchChunkManager = this.owningDB.globalChunkManager.getOrCreateChunkManagerForBranch(branch)
        this.rebuildIndexForChunk(branchChunkManager.headChunk)
    }


}
