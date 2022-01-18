package org.chronos.chronodb.exodus.manager

import org.apache.commons.lang3.tuple.Pair
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.TextCompare
import org.chronos.chronodb.api.key.ChronoIdentifier
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.secondaryindex.*
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexEntryConsumer
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification
import org.chronos.chronodb.internal.impl.index.AbstractIndexManager
import org.chronos.chronodb.internal.impl.index.IndexerWorkloadSorter
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.index.cursor.DeltaResolvingScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import java.util.*
import kotlin.math.min
import kotlin.reflect.KClass

class ExodusIndexManager : AbstractIndexManager<ExodusChronoDB> {

    private val backend: ExodusIndexManagerBackend

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB) : super(owningDB) {
        this.backend = ExodusIndexManagerBackend(owningDB)
        this.initializeIndicesFromDisk()
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun reindexAll(force: Boolean) {
        this.owningDB.lockExclusive().use {
            if (this.getDirtyIndices().isEmpty() && !force) {
                // no indices are dirty -> no need to re-index
                return
            }
            this.backend.rebuildIndexOnAllChunks(force)
            val allIndices = this.indexTree.getAllIndices().asSequence().filterIsInstance<SecondaryIndexImpl>().toSet()
            allIndices.forEach { it.dirty = false }
            this.backend.persistIndices(allIndices)
            this.clearQueryCache()
            this.owningDB.statisticsManager.clearBranchHeadStatistics()
        }
    }


    override fun index(identifierToOldAndNewValue: Map<ChronoIdentifier, Pair<Any?, Any?>>) {
        if (identifierToOldAndNewValue.isEmpty()) {
            // no workload to index
            return
        }
        if (this.indexTree.isEmpty()) {
            // no indices registered
            return
        }
        this.owningDB.lockNonExclusive().use {
            IndexingProcess().index(identifierToOldAndNewValue)
        }
    }

    override fun performIndexQuery(timestamp: Long, branch: Branch, keyspace: String, searchSpec: SearchSpecification<*, *>): Set<String> {
        this.owningDB.lockNonExclusive().use {
            // check if we are dealing with a negated search specification that accepts empty values.
            if (searchSpec.condition.isNegated && searchSpec.condition.acceptsEmptyValue()) {
                // the search spec is a negated condition that accepts the empty value.
                // To resolve this condition:
                // - Call keySet() on the target keyspace
                // - query the index with the non-negated condition
                // - subtract the matches from the keyset
                val keySet = this.owningDB.tx(branch.name, timestamp).keySet(keyspace)
                val nonNegatedSearch = searchSpec.negate()
                val scanResult = this.backend.performSearch(timestamp, branch, keyspace, nonNegatedSearch)
                // subtract the matches from the keyset
                for (resultEntry in scanResult) {
                    keySet.remove(resultEntry)
                }
                return Collections.unmodifiableSet(keySet)
            } else {
                val scanResult = this.backend.performSearch(timestamp, branch, keyspace, searchSpec)
                return Collections.unmodifiableSet(scanResult)
            }
        }

    }

    fun <T : Any> allEntries(branch: String, keyspace: String, propertyName: String, type: KClass<T>, consumer: IndexEntryConsumer<T>) {
        this.backend.allEntries(branch, keyspace, propertyName, type, consumer)
    }

    fun reindexHeadRevision(branchName: String) {
        this.backend.rebuildIndexOnHeadChunk(branchName)
    }

    override fun createCursorInternal(
        branch: Branch,
        timestamp: Long,
        index: SecondaryIndex,
        keyspace: String,
        indexName: String,
        sortOrder: Order,
        textCompare: TextCompare,
        keys: Set<String>?
    ): IndexScanCursor<*> {
        val globalChunkManager = this.owningDB.globalChunkManager
        val chunk = globalChunkManager.getChunkManagerForBranch(branch.name).getChunkForTimestamp(timestamp)
            ?: throw IllegalArgumentException("There is no chunk for timestamp ${timestamp} on branch ${branch.name}!")

        val scanCursor = if (chunk.isDeltaChunk && index.parentIndexId != null) {
            // we require delta computation

            // create the cursor on the parent
            val parentBranch = branch.origin
            val parentTimestamp = min(timestamp, branch.branchingTimestamp)

            val parentCursor = this.createCursor<Comparable<*>>(parentTimestamp, parentBranch, keyspace, indexName, sortOrder, textCompare, keys)

            val tx = globalChunkManager.openReadOnlyTransactionOn(chunk.indexDirectory)
            val rawCursor = ExodusChunkIndex.createRawIndexCursor<Comparable<*>>(tx, keyspace, index, sortOrder, textCompare)
            val scanCursor = DeltaResolvingScanCursor(parentCursor, timestamp, rawCursor)
            scanCursor.onClose { tx.rollback() }
            scanCursor
        } else {
            // simple case: only scan one chunk.
            val tx = globalChunkManager.openReadOnlyTransactionOn(chunk.indexDirectory)
            val scanCursor = ExodusChunkIndex.createIndexScanCursor<Comparable<*>>(tx, keyspace, timestamp, index, sortOrder, textCompare)
            scanCursor.onClose { tx.rollback() }
            scanCursor
        }
        return if (keys != null) {
            scanCursor.filter {
                it.second in keys
            }
        } else {
            scanCursor
        }
    }


    inner class IndexingProcess {

        private var currentTimestamp = -1L
        private var indexModifications: ExodusIndexModifications? = null

        fun index(identifierToValue: Map<ChronoIdentifier, Pair<Any?, Any?>>) {
            // build the indexer workload. The primary purpose is to sort the entries
            // of the map in an order suitable for processing.
            val workload = IndexerWorkloadSorter.sort(identifierToValue)
            // get the iterator over the workload
            val iterator = workload.iterator()
            // iterate over the workload
            while (iterator.hasNext()) {
                val entry = iterator.next()
                // unwrap the chrono identifier and the value to index associated with it
                val chronoIdentifier = entry.key
                // check if we need to perform any periodic tasks
                this.checkCurrentTimestamp(chronoIdentifier.timestamp)
                // index the single entry
                val oldAndNewValue = identifierToValue[chronoIdentifier]!!
                val oldValue = oldAndNewValue.left
                val newValue = oldAndNewValue.right
                this.indexSingleEntry(chronoIdentifier, oldValue, newValue)
            }
            // apply any remaining index modifications
            if (this.indexModifications != null && this.indexModifications!!.isNotEmpty) {
                this@ExodusIndexManager.backend.applyModifications(this.indexModifications!!)
            }
        }


        private fun checkCurrentTimestamp(nextTimestamp: Long) {
            if (this.currentTimestamp < 0 || this.currentTimestamp != nextTimestamp) {
                // the timestamp of the new work item is different from the one before. We need
                // to apply any index modifications (if any) and open a new modifications object
                if (this.indexModifications != null && this.indexModifications!!.isNotEmpty) {
                    this@ExodusIndexManager.backend.applyModifications(this.indexModifications!!)
                }
                this.currentTimestamp = nextTimestamp
                this.indexModifications = ExodusIndexModifications(nextTimestamp)
            }
        }

        private fun indexSingleEntry(identifier: ChronoIdentifier, oldValue: Any?, newValue: Any?) {
            val indices = this@ExodusIndexManager.indexTree.getAllIndices()
            val diff = IndexingUtils.calculateDiff(indices, oldValue, newValue)
            diff.changedIndices.forEach { index ->
                diff.getAdditions(index).forEach { addedValue ->
                    this.indexModifications!!.addEntryAddition(
                        ExodusIndexEntryAddition(
                            branch = identifier.branchName,
                            index = index,
                            keyspace = identifier.keyspace,
                            key = identifier.key,
                            value = addedValue
                        )
                    )
                }
                diff.getRemovals(index).forEach { removedValue ->
                    this.indexModifications!!.addEntryTermination(
                        ExodusIndexEntryTermination(
                            branch = identifier.branchName,
                            index = index,
                            keyspace = identifier.keyspace,
                            key = identifier.key,
                            value = removedValue
                        )
                    )
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun loadIndicesFromPersistence(): Set<SecondaryIndexImpl> {
        return this.backend.loadIndicesFromPersistence() as Set<SecondaryIndexImpl>
    }

    override fun deleteIndexInternal(index: SecondaryIndexImpl) {
        this.backend.deleteIndex(index)
    }

    override fun deleteAllIndicesInternal() {
        this.backend.deleteAllIndices()
    }

    override fun saveIndexInternal(index: SecondaryIndexImpl) {
        this.backend.persistIndex(index)
    }

    override fun saveIndicesInternal(indices: Set<SecondaryIndexImpl>) {
        this.backend.persistIndices(indices)
    }

    override fun rollback(index: SecondaryIndexImpl, timestamp: Long) {
        this.rollback(setOf(index), timestamp)
    }

    override fun rollback(indices: Set<SecondaryIndexImpl>, timestamp: Long) {
        this.backend.rollback(indices, timestamp)
    }

    override fun rollback(indices: Set<SecondaryIndexImpl>, timestamp: Long, keys: Set<QualifiedKey>) {
        this.backend.rollback(indices, timestamp, keys)
    }

}