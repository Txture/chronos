package org.chronos.chronodb.exodus.migration

import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import com.google.common.collect.Sets
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.cast
import org.chronos.chronodb.exodus.kotlin.ext.mapSingle
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.manager.BranchMetadataIndex
import org.chronos.chronodb.exodus.secondaryindex.ExodusSecondaryIndex
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.migration.ChronosMigration
import org.chronos.chronodb.internal.api.migration.annotations.Migration
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.util.MultiMapUtil
import org.chronos.common.serialization.KryoManager
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

@Suppress("DEPRECATION")
@Migration(from = "1.1.18", to = "1.2.0")
class ExodusMigration1_1_18_to_1_2_0 : ChronosMigration<ExodusChronoDB> {

    private companion object {

        private val log = KotlinLogging.logger {}

    }

    override fun execute(chronoDB: ExodusChronoDB) {
        val newIndices = chronoDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val indexersMap = this.loadIndexersMap(tx)
            val dirtyStateMap = this.loadIndexStates(tx)

            val newIndices = Sets.union(indexersMap.keySet(), dirtyStateMap.keys).asSequence()
                .mapNotNull { indexName ->
                    val indexers = indexersMap[indexName]
                    // declare all indices as dirty, as this migration requires a
                    // reindexing.
                    convertToNewSecondaryIndex(indexName, indexers, isDirty = true)
                }.toSet()

            tx.truncateStore(ChronoDBStoreLayout.STORE_NAME__INDEXERS)
            tx.truncateStore(ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY)

            tx.commit()

            newIndices
        }

        // inherit the indices to the child branches
        val indicesToSave = inheritIndicesToChildBranches(chronoDB, newIndices)

        // delete the old index directories (we've marked the indices as dirty already)
        this.deleteSecondaryIndexDirectories(chronoDB)

        chronoDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            this.saveIndexers(tx, indicesToSave.asSequence().map { it.id to it.toByteArray() }.toMap())
            tx.commit()
        }
    }

    private fun inheritIndicesToChildBranches(chronoDB: ExodusChronoDB, masterIndices: Set<SecondaryIndex>): Set<SecondaryIndex> {
        if(masterIndices.isEmpty()){
            return emptySet()
        }
        val branchInfo = this.getBranchInfo(chronoDB)
        val branchesByName = branchInfo.associateBy { it.name }
        val branchToChildren = branchInfo.groupBy { branchesByName[it.parentName] }
        val toVisit = LinkedBlockingQueue<IBranchMetadata>()
        toVisit.put(branchesByName.getValue(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER))
        val branchToIndices = mutableMapOf<String, Set<SecondaryIndex>>()
        branchToIndices[ChronoDBConstants.MASTER_BRANCH_IDENTIFIER] = masterIndices
        while (toVisit.isNotEmpty()) {
            val currentBranch = toVisit.poll()
            val parentBranchName = currentBranch.parentName
            if (parentBranchName != null) {
                // inherit indices from parent
                val parentIndices = branchToIndices[parentBranchName] ?: emptySet()
                val currentBranchIndices = parentIndices.asSequence()
                    .filter { it.validPeriod.contains(currentBranch.branchingTimestamp) }
                    .map { parentIndex ->
                        SecondaryIndexImpl(
                            id = UUID.randomUUID().toString(),
                            name = parentIndex.name,
                            indexer = parentIndex.indexer,
                            validPeriod = Period.createRange(currentBranch.branchingTimestamp, parentIndex.validPeriod.upperBound),
                            branch = currentBranch.name,
                            parentIndexId = parentIndex.id,
                            dirty = true,
                            options = parentIndex.inheritableOptions
                        )
                    }.toSet()
                branchToIndices[currentBranch.name] = currentBranchIndices
            }
            val childBranches = branchToChildren[currentBranch] ?: emptyList()
            childBranches.forEach(toVisit::put)
        }
        return branchToIndices.values.asSequence().flatten().toSet()
    }

    private fun deleteSecondaryIndexDirectories(chronoDB: ExodusChronoDB) {
        val branches = this.getBranchInfo(chronoDB)
        for (branch in branches) {
            chronoDB.globalChunkManager.dropAllSecondaryIndexFiles(branch)
        }
    }

    private fun convertToNewSecondaryIndex(indexName: String, indexers: Set<Indexer<*>>?, isDirty: Boolean): SecondaryIndexImpl? {
        val hadMultipleIndexers = indexers != null && indexers.size > 1
        val indexer = reduceMultipleIndexersToSingle(indexName, indexers)
            ?: return null
        return SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = indexName,
            indexer = indexer,
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            // if we had multiple indexers, now we have only one -> reindex.
            dirty = isDirty || hadMultipleIndexers,
            options = emptySet()
        )
    }

    private fun reduceMultipleIndexersToSingle(
        indexName: String,
        indexers: Set<Indexer<*>>?
    ): Indexer<*>? {
        if (indexers.isNullOrEmpty()) {
            return null
        }
        if (indexers.size == 1) {
            return indexers.single()
        }
        log.warn {
            "Multiple indexers were registered for name '${indexName}'. " +
                "This is not supported in Chronos 1.2.0 and onwards. " +
                "Keeping one index intact, removing the others (preferring " +
                "String over Double over Long indices)."
        }
        val stringIndexers = indexers.filterIsInstance<StringIndexer>()
        if (stringIndexers.isNotEmpty()) {
            return stringIndexers.first()
        }
        val doubleIndexers = indexers.filterIsInstance<DoubleIndexer>()
        if (doubleIndexers.isNotEmpty()) {
            return doubleIndexers.first()
        }
        val longIndexers = indexers.filterIsInstance<LongIndexer>()
        if (longIndexers.isNotEmpty()) {
            return longIndexers.first()
        }
        // no idea what kind of indexer this is...
        log.warn {
            "ChronoDB migration was unable to identify indexer type for " +
                "index '${indexName}'. Dropping this index."
        }
        return null
    }

    private fun deserialize(bytes: ByteArray): Any? {
        return KryoManager.deserialize<Any?>(bytes)
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadIndexersMap(tx: ExodusTransaction): SetMultimap<String, Indexer<*>> {
        val map: Map<String, Set<Indexer<*>>> = this.getIndexersSerialForm(tx).mapSingle { serializedForm ->
            this.deserialize(serializedForm) as Map<String, Set<Indexer<*>>>?
        } ?: return HashMultimap.create()
        // we need to convert our internal map representation back into its multimap form
        return MultiMapUtil.copyToMultimap(map)
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadIndexStates(tx: ExodusTransaction): Map<String, Boolean> {
        return this.getIndexDirtyFlagsSerialForm(tx)
            ?.let { this.deserialize(it) as Map<String, Boolean> }
            ?: emptyMap()
    }

    private fun getIndexersSerialForm(tx: ExodusTransaction): ByteArray? {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        return tx.get(indexName, key)?.toByteArray()
    }

    private fun getIndexDirtyFlagsSerialForm(tx: ExodusTransaction): ByteArray? {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        return tx.get(indexName, key)?.toByteArray()
    }

    private fun saveIndexers(tx: ExodusTransaction, idToSerialForm: Map<String, ByteArray>) {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        for ((id, serialForm) in idToSerialForm) {
            tx.put(indexName, id, serialForm.toByteIterable())
        }
    }

    private fun SecondaryIndex.toByteArray(): ByteArray {
        val serializableForm = ExodusSecondaryIndex(this)
        return KryoManager.serialize(serializableForm)
    }

    private fun getBranchInfo(chronoDB: ExodusChronoDB): List<IBranchMetadata> {
        return chronoDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            BranchMetadataIndex.values(tx).asSequence()
                .map(chronoDB.serializationManager::deserialize)
                .cast(IBranchMetadata::class)
                .toList()
        }
    }
}
