package org.chronos.chronodb.exodus.secondaryindex

import com.google.common.collect.*
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import mu.KotlinLogging
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.parseAsUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification
import org.chronos.chronodb.internal.impl.index.diff.IndexValueDiff
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.chronos.common.serialization.KryoManager
import java.nio.file.Files
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.math.max
import kotlin.math.min
import kotlin.reflect.KClass

class ExodusIndexManagerBackend {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    companion object {

        private const val REINDEX_FLUSH_INTERVAL = 25_000

        private val log = KotlinLogging.logger {}

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

    fun loadIndicesFromPersistence(): Set<SecondaryIndex> {
        return this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            this.loadIndexSet(tx)
        }
    }

    fun persistIndices(indices: Set<SecondaryIndex>) {
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            this.persistIndexSet(tx, indices)
            tx.commit()
        }
    }

    fun persistIndex(index: SecondaryIndex) {
        this.persistIndices(setOf(index))
        this.deleteChunkIndices(setOf(index))
    }

    fun deleteIndex(index: SecondaryIndex) {
        // first, delete the indexers
        this.owningDB.lockNonExclusive().use {
            this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
                tx.delete(ChronoDBStoreLayout.STORE_NAME__INDEXERS, index.id)
                tx.commit()
            }
        }
        // we do not immediately cleanup current and old chunks; specific index is not written/updated from now on
    }

    fun deleteAllIndices() {
        // first, delete the indexers
        this.owningDB.lockNonExclusive().use {
            this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
                tx.truncateStore(ChronoDBStoreLayout.STORE_NAME__INDEXERS)
                tx.commit()
            }
        }
        // delete all index chunks
        this.deleteAllChunkIndices()
    }

    fun rollback(indices: Set<SecondaryIndex>, timestamp: Long) {
        requireNonNegative(timestamp, "timestamp")
        this.rollbackInternal(indices, timestamp, null)
    }

    fun rollback(indices: Set<SecondaryIndex>, timestamp: Long, keys: Set<QualifiedKey>) {
        requireNonNegative(timestamp, "timestamp")
        this.rollbackInternal(indices, timestamp, keys)
    }

    private fun rollbackInternal(indices: Set<SecondaryIndex>, timestamp: Long, keys: Set<QualifiedKey>?) {
        requireNonNegative(timestamp, "timestamp")
        val gcm = this.owningDB.globalChunkManager
        for (index in indices) {
            val headChunk = gcm.getChunkManagerForBranch(index.branch).headChunk
            check(headChunk.validPeriod.contains(timestamp)) {
                "Cannot roll back branch ${index.branch} to timestamp $timestamp - " +
                    "it is not within the validity period of the HEAD chunk (${headChunk.validPeriod})!"
            }
            gcm.openReadWriteTransactionOn(headChunk.indexDirectory).use { tx ->
                val rolledBackIndices = mutableSetOf<Pair<SecondaryIndex, KClass<*>>>()
                indices.forEach { index ->
                    when (index.indexer) {
                        is StringIndexer -> {
                            if (!rolledBackIndices.contains(Pair(index, String::class))) {
                                SecondaryStringIndexStore.rollback(tx, index.id, timestamp, keys)
                                rolledBackIndices.add(index to String::class)
                            }
                        }
                        is DoubleIndexer -> {
                            if (!rolledBackIndices.contains(Pair(index, Double::class))) {
                                SecondaryDoubleIndexStore.rollback(tx, index.id, timestamp, keys)
                                rolledBackIndices.add(index to Double::class)
                            }
                        }
                        is LongIndexer -> {
                            if (!rolledBackIndices.contains(Pair(index, Long::class))) {
                                SecondaryLongIndexStore.rollback(tx, index.id, timestamp, keys)
                                rolledBackIndices.add(Pair(index, Long::class))
                            }
                        }
                        else -> throw IllegalArgumentException("Unknown indexer type: ${index.indexer.javaClass.name}")
                    }
                    // flush after each indexer to write changes to disk (but not commit them yet)
                    tx.flush()

                }
                tx.commit()
            }
        }
    }

    fun rebuildIndexOnAllChunks(force: Boolean) {
        val indices = if (force) {
            val allIndices = this.owningDB.indexManager.getIndices()
            // drop ALL index files (more efficient implementation than the override with 'dirtyIndices' parameter)
            this.deleteAllChunkIndices()
            allIndices
        } else {
            val dirtyIndices = this.owningDB.indexManager.getDirtyIndices()
            if(dirtyIndices.isEmpty()){
                return
            }
            // delete all index data we have for those indices
            this.owningDB.globalChunkManager.dropSecondaryIndexFiles(dirtyIndices)
            dirtyIndices
        }
        if (indices.isEmpty()) {
            // we have no secondary indices -> we're done.
            return
        }
        // group the indices by branch, and reindex one branch at a time.
        val indicesByBranch = indices.groupBy { it.branch }
        for ((branch, branchIndices) in indicesByBranch) {
            // check which chunks we need for the reindexing. If none of the
            // indices on the branch overlap the period of a chunk, ignore the chunk.
            val indexingPeriod = branchIndices.overallPeriod
            val bcm = this.owningDB.globalChunkManager.getChunkManagerForBranch(branch)
            val chunks = bcm.getChunksForPeriod(indexingPeriod)
                // the overall period may be e.g. 1000 - infinity, but maybe
                // we have one index from 1000-2000, and one from 8000 - infinity. The "holes"
                // in between may contain chunks which we can skip, so we filter again here.
                .filter { chunk -> branchIndices.any { it.validPeriod.overlaps(chunk.validPeriod) } }

            // build the baselines on the delta chunk, if necessary
            val deltaChunk = chunks.firstOrNull { it.isDeltaChunk }
            this.createIndexBaselinesOnDeltaChunkIfNecessary(deltaChunk, branchIndices)

            for (chunk in chunks) {
                this.rebuildIndexForChunk(chunk, branchIndices.toSet())
            }
        }
    }

    private val Collection<SecondaryIndex>.overallPeriod: Period
        get() {
            val minLowerBound = minOf { it.validPeriod.lowerBound }
            val maxUpperBound = maxOf { it.validPeriod.upperBound }
            return Period.createRange(minLowerBound, maxUpperBound)
        }

    /**
     * Creates the "baseline" for the given indices.
     *
     * Baselines contain all index values at the baseline timestamp. We require them only if
     * the chunk in question is a delta-chunk and the index is non-inherited. For those cases,
     * we need to artificially construct the baseline for the index, because a scan of the delta
     * chunk will not produce all index values.
     *
     * Creating a baseline is done by iterating over all entries in the keyset, for each keyspace,
     * and indexing them. This is rather inefficient, but at least resolves the branches recursively
     * for us.
     *
     * @param deltaChunk The delta chunk to compute the index baselines for.
     * @param indices The indices to consider.
     */
    private fun createIndexBaselinesOnDeltaChunkIfNecessary(deltaChunk: ChronoChunk?, indices: Collection<SecondaryIndex>) {
        if (deltaChunk == null || !deltaChunk.isDeltaChunk) {
            return
        }

        val unbasedIndices = indices.asSequence()
            // do not include inherited indices (for those we need no baseline, because the queries
            // will be redirected to the parent index anyways).
            .filter { it.parentIndexId == null }
            // only consider the indices on the same branch as the chunk
            .filter { it.branch == deltaChunk.branchName }
            // only consider indices that have an overlapping valid period with the chunk
            .filter { it.validPeriod.overlaps(deltaChunk.validPeriod) }
            .toSet()
        if (unbasedIndices.isEmpty()) {
            return
        }
        this.owningDB.globalChunkManager.openReadWriteTransactionOn(deltaChunk.indexDirectory).use { indexTx ->
            var batchSize = 0
            val unbasedIndexGroup = unbasedIndices.groupBy { max(deltaChunk.validPeriod.lowerBound, it.validPeriod.lowerBound) }
            for ((startTimestamp, indicesInGroup) in unbasedIndexGroup) {
                val chronoDbTransaction = this.owningDB.tx(deltaChunk.branchName, startTimestamp)
                for (keyspace in chronoDbTransaction.keyspaces()) {
                    val keySet = chronoDbTransaction.keySet(keyspace)
                    for (key in keySet) {
                        val modifications = ExodusIndexModifications(startTimestamp)
                        val value = chronoDbTransaction.get<Any?>(keyspace, key)
                            ?: continue // safeguard, can't happen
                        for (index in indicesInGroup) {
                            val indexValues = index.getIndexedValuesForObject(value)
                            for (indexValue in indexValues) {
                                modifications.addEntryAddition(ExodusIndexEntryAddition(
                                    branch = deltaChunk.branchName,
                                    index = index,
                                    keyspace = keyspace,
                                    key = key,
                                    value = indexValue
                                ))
                                batchSize++
                            }
                        }
                        ExodusChunkIndex.applyModifications(indexTx, modifications, startTimestamp)
                        if (batchSize >= REINDEX_FLUSH_INTERVAL) {
                            indexTx.flush()
                            batchSize = 0
                        }
                    }
                }
            }
            indexTx.flush()
            indexTx.commit()
        }
    }

    fun applyModifications(indexModifications: ExodusIndexModifications) {
        val gcm = this.owningDB.globalChunkManager
        indexModifications.groupByBranch().forEach { (branchName, modifications) ->
            val chunk = gcm.getChunkManagerForBranch(branchName).getChunkForTimestamp(modifications.changeTimestamp)
                ?: throw IllegalStateException(
                    "Cannot apply index modifications - there is no chunk for this time period! " +
                        "Change timestamp: ${modifications.changeTimestamp}, Branch: '${branchName}'"
                )
            if (!chunk.indexDirectory.exists()) {
                Files.createDirectory(chunk.indexDirectory.toPath())
            }
            val branchingTimestamp = this.owningDB.branchManager.getBranch(branchName).branchingTimestamp
            val lowerBound = max(branchingTimestamp, chunk.validPeriod.lowerBound)
            gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
                ExodusChunkIndex.applyModifications(tx, modifications, lowerBound)
                tx.commit()
            }
        }
    }

    fun <T> performSearch(timestamp: Long, branch: Branch, keyspace: String, searchSpec: SearchSpecification<T, *>): Set<String> {
        requireNonNegative(timestamp, "timestamp")
        val gcm = this.owningDB.globalChunkManager

        val indices = this.owningDB.indexManager.getParentIndicesRecursive(searchSpec.index, true)
        val branchesByName = indices.asSequence()
            .map { this.owningDB.branchManager.getBranch(it.branch) }
            .associateBy { it.name }

        val branches = indices.asSequence().map { branchesByName.getValue(it.branch) }.toList().asReversed()
        val branchToIndex = indices.associateBy { branchesByName.getValue(it.branch) }

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
            val branchSearchSpec = if (currentBranch == branch) {
                searchSpec
            } else {
                val branchLocalIndex = branchToIndex.getValue(currentBranch)
                searchSpec.onIndex(branchLocalIndex)
            }
            val branchName = currentBranch.name
            // Important note: if the branch we are currently dealing with is NOT the branch that we are
            // querying, but a PARENT branch instead, we must use the MINIMUM of
            // (branching timestamp; request timestamp),
            // because the branch might have changes that are AFTER the branching timestamp but BEFORE
            // the request timestamp, and we don't want to see those in the result.
            val scanTimestamp = when {
                currentBranch != branch -> min(timestamp, branchOutTimestamps.getValue(currentBranch))
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
                ?: continue // there is no chunk here... we have no information about this branch/timestamp combination.
            if (!resultMap.isEmpty) {
                gcm.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                    val terminations = ExodusChunkIndex.scanForTerminations(tx, scanTimestamp, keyspace, branchSearchSpec)
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
                    ExodusChunkIndex.scanForResults(tx, scanTimestamp, keyspace, branchSearchSpec)
                }
                val set = Sets.newHashSetWithExpectedSize<String>(result.size)
                for (entry in result) {
                    set.add(entry.primaryKey)
                }
                return set
            }
            // find the branch-local matches of the given branch.
            gcm.openReadOnlyTransactionOn(chunk.indexDirectory).use { tx ->
                val additions = ExodusChunkIndex.scanForResults(tx, scanTimestamp, keyspace, branchSearchSpec)
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

    private fun rebuildIndexForChunk(chunk: ChronoChunk, indices: Set<SecondaryIndex>) {
        if (indices.isEmpty()) {
            return
        }

        val gcm = this.owningDB.globalChunkManager

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
                    chunkTx.withCursorOn(matrixName) { cursor ->
                        performReindexing(cursor, branchName, keyspaceName, chunk, indices, indexTx)
                    }
                }
            }
            indexTx.commit()
        }
    }

    private fun performReindexing(
        cursor: Cursor,
        branchName: String,
        keyspaceName: String,
        chunk: ChronoChunk,
        indices: Set<SecondaryIndex>,
        indexTx: ExodusTransaction
    ) {
        if (indices.isEmpty()) {
            return
        }
        var previousKey: String? = null
        var previousValue: Any? = null
        var previousIndexValues: SetMultimap<SecondaryIndex, Any>? = null
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
                    if (chunk.isDeltaChunk) {
                        // this is a deletion of an entry that doesn't belong to our
                        // branch -> we need to fetch it from the origin branch to
                        // calculate the index entries to terminate.
                        val previousObject: Any? = this.owningDB.tx(branchName, tKey.timestamp - 1).get(keyspaceName, tKey.key)
                        if (previousObject != null) { // just a safeguard; NULL can't really happen.
                            val indexValues = IndexingUtils.getIndexedValuesForObject(indices, previousObject)
                            terminateAll(branchName, keyspaceName, tKey.key, indexValues, indexModifications)
                        } else {
                            log.warn {
                                "Detected unexpected case during secondary indexing. " +
                                    "The entry [${branchName}].[${keyspaceName}].[${tKey.key}]@${tKey.timestamp} is a deletion, " +
                                    "but a temporal GET to the predecessor produced NULL as well. Skipping this entry."
                            }
                        }
                    } else {
                        // this is not a delta-chunk and the first entry on this key is a deletion? Weird.
                        log.warn {
                            "Detected unexpected case during secondary indexing. " +
                                "The entry [${branchName}].[${keyspaceName}].[${tKey.key}]@${tKey.timestamp} is a deletion, " +
                                "but it has no preceding insert. Skipping this entry."
                        }
                    }
                }
                previousIndexValues = null
            } else {
                // insertion or update
                val indexValues = IndexingUtils.getIndexedValuesForObject(indices, value)
                val indexingDiff = when {
                    tKey.key == previousKey && previousIndexValues != null -> {
                        // we have a direct update to our predecessor entry
                        IndexingUtils.calculateDiff(indices, previousValue, value)
                    }
                    else -> {
                        // this is the first insert of this key in this chunk. If we are dealing
                        // with a delta chunk, we need to consider the predecessor entry in the
                        // origin chunk too.
                        val previousObject: Any? = when {
                            // the entry may override an entry of the origin branch. Fetch the original value (if any)
                            // Compiler note: it is *crucial* that we state the "Any?" type parameter in this "get" call because of a bug
                            // in the kotlin compiler. See: https://youtrack.jetbrains.com/issue/KT-29629
                            chunk.isDeltaChunk -> this.owningDB.tx(branchName, max(0L, tKey.timestamp - 1)).get<Any?>(keyspaceName, tKey.key)
                            // this is not a delta chunk, there is no previous object we need to concern ourselves with.
                            else -> null
                        }
                        // we have a predecessor version (which may be NULL), calculate its index values
                        IndexingUtils.calculateDiff(indices, previousObject, value)
                    }
                }
                // transform the diff into index modifications
                applyIndexingDiffToModifications(indexingDiff, indexModifications, branchName, keyspaceName, tKey, chunk)
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

    private fun applyIndexingDiffToModifications(
        indexingDiff: IndexValueDiff,
        indexModifications: ExodusIndexModifications,
        branchName: String,
        keyspaceName: String,
        tKey: UnqualifiedTemporalKey,
        chunk: ChronoChunk
    ) {
        val timestamp = indexModifications.changeTimestamp
        for (changedIndex in indexingDiff.changedIndices) {
            if (changedIndex.validPeriod.isBefore(timestamp)) {
                continue
            }
            if (chunk.isDeltaChunk && changedIndex.parentIndexId == null
                && max(changedIndex.validPeriod.lowerBound, chunk.validPeriod.lowerBound) == timestamp) {
                // this is an unbased index and the modifications refer exactly to the baseline
                // -> ignore those modifications
                continue
            }
            for (addedValue in indexingDiff.getAdditions(changedIndex)) {
                indexModifications.addEntryAddition(
                    ExodusIndexEntryAddition(
                        branch = branchName,
                        index = changedIndex,
                        keyspace = keyspaceName,
                        key = tKey.key,
                        value = addedValue
                    )
                )
            }
            for (removedValue in indexingDiff.getRemovals(changedIndex)) {
                indexModifications.addEntryTermination(
                    ExodusIndexEntryTermination(
                        branch = branchName,
                        index = changedIndex,
                        keyspace = keyspaceName,
                        key = tKey.key,
                        value = removedValue
                    )
                )
            }
        }
    }

    private fun terminateAll(branchName: String, keyspaceName: String, key: String, previousIndexValues: SetMultimap<SecondaryIndex, Any>, indexModifications: ExodusIndexModifications) {
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


    private fun deleteChunkIndices(indices: Set<SecondaryIndex>) {
        val gcm = this.owningDB.globalChunkManager
        val branchToIndices = indices.groupBy { it.branch }
        for ((branch, branchIndices) in branchToIndices) {
            val indexIds = branchIndices.asSequence().map { it.id }.toSet()
            val bcm = gcm.getChunkManagerForBranch(branch)
            for (chunk in bcm.getAllChunks()) {
                gcm.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
                    for (storeName in tx.getAllStoreNames()) {
                        val stringIndexId = SecondaryStringIndexStore.getIndexIdForStoreName(storeName)
                        val longIndexId = SecondaryLongIndexStore.getIndexIdForStoreName(storeName)
                        val doubleIndexId = SecondaryDoubleIndexStore.getIndexIdForStoreName(storeName)
                        when {
                            stringIndexId in indexIds -> tx.removeStore(storeName)
                            longIndexId in indexIds -> tx.removeStore(storeName)
                            doubleIndexId in indexIds -> tx.removeStore(storeName)
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
                this.deleteAllChunkIndices(chunk)
            }
        }
    }

    private fun deleteAllChunkIndices(chunk: ChronoChunk) {
        this.owningDB.globalChunkManager.openReadWriteTransactionOn(chunk.indexDirectory).use { tx ->
            for (storeName in tx.getAllStoreNames()) {
                tx.removeStore(storeName)
            }
            tx.commit()
        }
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
    private fun loadIndexSet(tx: ExodusTransaction): Set<SecondaryIndex> {
        return this.getIndexersSerialForm(tx).asSequence()
            .map { it.parseSecondaryIndex() }
            .toSet()
    }

    private fun persistIndexSet(tx: ExodusTransaction, indices: Set<SecondaryIndex>) {
        this.saveIndexers(tx, indices.asSequence().map { it.id to it.toByteArray() }.toMap())
    }

    private fun getIndexersSerialForm(tx: ExodusTransaction): List<ByteArray> {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        val resultList = mutableListOf<ByteArray>()
        tx.withCursorOn(indexName) { cursor ->
            while (cursor.next) {
                resultList += cursor.value.toByteArray()
            }
        }
        return resultList
    }

    private fun saveIndexers(tx: ExodusTransaction, idToSerialForm: Map<String, ByteArray>) {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        for ((id, serialForm) in idToSerialForm) {
            tx.put(indexName, id, serialForm.toByteIterable())
        }
    }

    fun rebuildIndexOnHeadChunk(branchName: String) {
        val branch = this.owningDB.branchManager.getBranch(branchName)
        val branchChunkManager = this.owningDB.globalChunkManager.getOrCreateChunkManagerForBranch(branch)
        val headChunk = branchChunkManager.headChunk
        val indices = this.owningDB.indexManager.getIndices(branch).asSequence()
            .filter { it.validPeriod.overlaps(headChunk.validPeriod) }
            .toSet()
        this.deleteAllChunkIndices(headChunk)
        this.rebuildIndexForChunk(headChunk, indices)
    }

    private fun SecondaryIndex.toByteArray(): ByteArray {
        val serializableForm = ExodusSecondaryIndex(this)
        return KryoManager.serialize(serializableForm)
    }

    private fun ByteArray.parseSecondaryIndex(): SecondaryIndex {
        val serializableForm = KryoManager.deserialize<ExodusSecondaryIndex>(this)
        return serializableForm.toSecondaryIndex()
    }
}
