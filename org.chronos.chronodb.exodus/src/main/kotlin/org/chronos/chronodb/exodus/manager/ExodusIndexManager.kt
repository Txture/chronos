package org.chronos.chronodb.exodus.manager

import org.apache.commons.lang3.tuple.Pair
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.key.ChronoIdentifier
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.secondaryindex.ExodusIndexEntryAddition
import org.chronos.chronodb.exodus.secondaryindex.ExodusIndexEntryTermination
import org.chronos.chronodb.exodus.secondaryindex.ExodusIndexManagerBackend
import org.chronos.chronodb.exodus.secondaryindex.ExodusIndexModifications
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexEntryConsumer
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanResultEntry
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification
import org.chronos.chronodb.internal.impl.index.AbstractBackendDelegatingIndexManager
import org.chronos.chronodb.internal.impl.index.IndexerWorkloadSorter
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import java.util.*
import kotlin.reflect.KClass

class ExodusIndexManager : AbstractBackendDelegatingIndexManager<ExodusChronoDB, ExodusIndexManagerBackend> {


    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB) : super(owningDB, ExodusIndexManagerBackend(owningDB))

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun reindex(indexName: String?) {
        this.reindexAll(false)
    }

    override fun reindexAll(force: Boolean) {
        this.owningDB.lockExclusive().use {
            if (this.dirtyIndices.isEmpty() && !force) {
                // no indices are dirty -> no need to re-index
                return
            }
            this.indexManagerBackend.rebuildIndexOnAllChunks()
            for (indexName in this.indexNames) {
                this.setIndexClean(indexName)
            }
            this.indexManagerBackend.persistIndexDirtyStates(this.indexNameToDirtyFlag)
            this.clearQueryCache()
            this.owningDB.statisticsManager.clearBranchHeadStatistics()
        }
    }

    override fun index(identifierToOldAndNewValue: MutableMap<ChronoIdentifier, Pair<Any?, Any?>>) {
        if (identifierToOldAndNewValue.isEmpty()) {
            // no workload to index
            return
        }
        if (this.indexNames.isEmpty()) {
            // no indices registered
            return
        }
        this.owningDB.lockNonExclusive().use {
            IndexingProcess().index(identifierToOldAndNewValue)
        }
    }

    override fun performIndexQuery(timestamp: Long, branch: Branch, keyspace: String, searchSpec: SearchSpecification<*,*>): Set<String> {
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
                val scanResult = this.indexManagerBackend.performSearch(timestamp, branch, keyspace, nonNegatedSearch)
                // subtract the matches from the keyset
                for (resultEntry in scanResult) {
                    keySet.remove(resultEntry)
                }
                return Collections.unmodifiableSet(keySet)
            } else {
                val scanResult = this.indexManagerBackend.performSearch(timestamp, branch, keyspace, searchSpec)
                return Collections.unmodifiableSet(scanResult)
            }
        }

    }

    fun <T: Any> allEntries(branch: String, keyspace: String, propertyName: String, type: KClass<T>, consumer: IndexEntryConsumer<T>){
        this.indexManagerBackend.allEntries(branch, keyspace, propertyName, type, consumer)
    }

    fun reindexHeadRevision(branchName: String) {
        this.indexManagerBackend.rebuildIndexOnHeadChunk(branchName)
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
                this@ExodusIndexManager.indexManagerBackend.applyModifications(this.indexModifications!!)
            }
        }


        private fun checkCurrentTimestamp(nextTimestamp: Long) {
            if (this.currentTimestamp < 0 || this.currentTimestamp != nextTimestamp) {
                // the timestamp of the new work item is different from the one before. We need
                // to apply any index modifications (if any) and open a new modifications object
                if (this.indexModifications != null && this.indexModifications!!.isNotEmpty) {
                    this@ExodusIndexManager.indexManagerBackend.applyModifications(this.indexModifications!!)
                }
                this.currentTimestamp = nextTimestamp
                this.indexModifications = ExodusIndexModifications(nextTimestamp)
            }
        }

        private fun indexSingleEntry(identifier: ChronoIdentifier, oldValue: Any?, newValue: Any?) {
            val indexNameToIndexers = this@ExodusIndexManager.indexNameToIndexers
            val diff = IndexingUtils.calculateDiff(indexNameToIndexers, oldValue, newValue)
            diff.changedIndices.forEach { indexName ->
                diff.getAdditions(indexName).forEach { addedValue ->
                    this.indexModifications!!.addEntryAddition(
                            ExodusIndexEntryAddition(
                                    branch = identifier.branchName,
                                    index = indexName,
                                    keyspace = identifier.keyspace,
                                    key = identifier.key,
                                    value = addedValue
                            )
                    )
                }
                diff.getRemovals(indexName).forEach { removedValue ->
                    this.indexModifications!!.addEntryTermination(
                            ExodusIndexEntryTermination(
                                    branch = identifier.branchName,
                                    index = indexName,
                                    keyspace = identifier.keyspace,
                                    key = identifier.key,
                                    value = removedValue
                            )
                    )
                }
            }
        }
    }
}