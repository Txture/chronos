package org.chronos.chronodb.internal.api.index

import org.apache.commons.lang3.tuple.Pair
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.IndexManager
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.key.ChronoIdentifier
import org.chronos.chronodb.internal.impl.index.IndexingOption
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache
import java.util.concurrent.Callable

interface IndexManagerInternal : IndexManager {

    /**
     * Indexes the given key-value pairs.
     *
     * This method is **not** considered part of the public API. Clients must not call this method directly!
     *
     * @param identifierToOldAndNewValue
     * The map of changed [ChronoIdentifier]s to their respective old and new values to index. Must not
     * be `null`, may be empty.
     */
    fun index(identifierToOldAndNewValue: Map<ChronoIdentifier, Pair<Any?, Any?>>)

    fun addIndex(
        indexName: String,
        branch: String,
        startTimestamp: Long,
        endTimestamp: Long,
        indexer: Indexer<*>,
        options: Collection<IndexingOption>
    ): SecondaryIndex

    fun addIndex(
        indexName: String,
        branch: Branch,
        startTimestamp: Long,
        endTimestamp: Long,
        indexer: Indexer<*>,
        options: Collection<IndexingOption>
    ): SecondaryIndex

    fun addIndices(indices: Set<SecondaryIndex>)

    fun getIndexQueryCache(): ChronoIndexQueryCache?

    /**
     * Marks *all* of the given [indices] as dirty.
     *
     * Indices which do not exist are silently ignored. This operation has no effect on indices which are already dirty.
     *
     * @param indices The indices to mark as dirty.
     *
     * @return `true` if at least one index was affected by this operation, or `false` if no index was affected.
     */
    fun markIndicesAsDirty(indices: Collection<SecondaryIndex>): Boolean

    fun markAllIndicesAsDirty(): Boolean

    fun getParentIndexOnBranch(index: SecondaryIndex, branch: Branch): SecondaryIndex

    fun getParentIndicesRecursive(index: SecondaryIndex, includeSelf: Boolean): List<SecondaryIndex>


    // =================================================================================================================
    // LOCKING
    // =================================================================================================================

    fun <T> withIndexReadLock(action: Callable<T>): T

    fun <T> withIndexReadLock(action: () -> T): T

    fun withIndexReadLock(action: Runnable)

    fun <T> withIndexWriteLock(action: Callable<T>): T

    fun <T> withIndexWriteLock(action: ()->T): T

    fun withIndexWriteLock(action: Runnable)

}