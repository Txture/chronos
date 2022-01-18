package org.chronos.chronodb.api

import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.internal.api.index.IndexManagerInternal
import org.chronos.chronodb.internal.api.query.ChronoDBQuery
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification

interface IndexManager {

    // =================================================================================================================
    // INDEX MANAGEMENT
    // =================================================================================================================

    fun getIndices(): Set<SecondaryIndex>

    fun getIndexById(id: String): SecondaryIndex?

    fun getIndices(branch: Branch): Set<SecondaryIndex>

    fun getIndices(branch: Branch, timestamp: Long): Set<SecondaryIndex>

    fun createIndex(): IndexBuilder {
        check(this is IndexManagerInternal) {
            "This IndexManager (Class: ${this.javaClass.name}) does not implement IndexManagerInternal. Cannot create index builder."
        }
        return IndexBuilderImpl(this)
    }

    fun deleteIndex(index: SecondaryIndex): Boolean

    fun deleteIndices(indices: Set<SecondaryIndex>): Boolean

    fun setIndexEndDate(index: SecondaryIndex, endTimestamp: Long): SecondaryIndex

    fun clearAllIndices(): Boolean

    // =================================================================================================================
    // INDEXING METHODS
    // =================================================================================================================

    /**
     * Re-indexes all dirty indices.
     *
     * Note that this is not a forced re-index. Only dirty indices will be re-indexed.
     *
     *
     *
     * Re-indexing is an expensive operation. Furthermore, re-indexing is an exclusive operation that does not permit
     * any concurrent reads and/or writes on the database.
     */
    fun reindexAll() {
        this.reindexAll(false)
    }

    /**
     * Re-indexes all indices.
     *
     * Re-indexing is an expensive operation. Furthermore, re-indexing is an exclusive operation that does not permit
     * any concurrent reads and/or writes on the database.
     *
     * @param force
     * Use `true` to force a re-build of all indices.
     */
    fun reindexAll(force: Boolean)

    /**
     * Checks if any index is dirty and requires re-indexing.
     *
     *
     *
     * If this method returns `true`, please consider calling [.reindexAll] to create a clean index.
     *
     * @return `true` if at least one index is dirty and requires re-indexing, otherwise `false`.
     */
    fun isReindexingRequired(): Boolean

    /**
     * Returns an immutable set of all index names that are currently referring to dirty indices.
     *
     * @return The immutable set of dirty secondary indices. May be empty, but never `null`.
     */
    fun getDirtyIndices(): Set<SecondaryIndex>

    // =================================================================================================================
    // INDEX QUERY METHODS
    // =================================================================================================================

    /**
     * Queries the index by providing a value description.
     *
     * @param timestamp
     * The timestamp at which the query takes place. Must not be negative.
     * @param branch
     * The branch to evaluate the query in. Must not be `null`. Must refer to an existing branch.
     * @param keyspace
     * The keyspace to evaluate the query in. Must not be `null`.
     * @param searchSpec
     * The search specification to fulfill. Must not be `null`.
     *
     * @return The set of keys that have a value assigned that matches the given description. May be empty, but never
     * `null`.
     */
    fun queryIndex(timestamp: Long, branch: Branch, keyspace: String,
                   searchSpec: SearchSpecification<*, *>): Set<String>

    /**
     * Evaluates the given [ChronoDBQuery].
     *
     * @param timestamp
     * The timestamp at which the evaluation takes place. Must not be negative.
     * @param branch
     * The branch to evaluate the query in. Must not be `null`.
     * @param query
     * The query to run. Must not be `null`. Must have been optimized before calling this method.
     *
     * @return An iterator on the keys that have values assigned which match the query. May be empty, but never
     * `null`.
     */
    fun evaluate(timestamp: Long, branch: Branch, query: ChronoDBQuery): Iterator<QualifiedKey>

    /**
     * Evaluates the given [ChronoDBQuery].
     *
     * @param timestamp
     * The timestamp at which the evaluation takes place. Must not be negative.
     * @param branch
     * The branch to evaluate the query in. Must not be `null`.
     * @param query
     * The query to run. Must not be `null`. Must have been optimized before calling this method.
     *
     * @return The number of key-value pairs in the database that match the given query. May be zero, but never
     * negative.
     */
    fun evaluateCount(timestamp: Long, branch: Branch, query: ChronoDBQuery): Long

    /**
     * Scans the index for the values associated with all keys in the given keyspace and groups them by key.
     *
     * If a key has no indexed value for the given [indexName], it will not appear in the
     * result map.
     *
     * @param timestamp The timestamp to execute the query on.
     * @param branch The branch to execute the query on.
     * @param keyspace The keyspace to query.
     * @param indexName The name of the index to query.
     *
     * @return A map from key to associated index values. Keys which do not exist in the database or have no associated values in the given index
     * will NOT appear in the map.
     */
    fun getIndexedValuesByKey(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String
    ): Map<String, Set<Comparable<*>>>


    /**
     * Scans the index for the values associated with the given keys groups them by key.
     *
     * Keys that points to non-existing entries will be ignored and will not appear in the
     * result map.
     *
     * If a key has no indexed value for the given [indexName], it will not appear in the
     * result map.
     *
     * @param timestamp The timestamp to execute the query on.
     * @param branch The branch to execute the query on.
     * @param keyspace The keyspace to query.
     * @param indexName The name of the index to query.
     * @param keys The keys to look up. If empty, the result map will be empty. Keys that have no associated value in the database will be ignored.
     *
     * @return A map from key to associated index values. Keys which do not exist in the database or have no associated values in the given index
     * will NOT appear in the map.
     */
    fun getIndexedValuesByKey(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
        keys: Set<String>
    ): Map<String, Set<Comparable<*>>>

    /**
     * Scans the index for the values associated with all keys in the given keyspace.
     *
     * Keys that points to non-existing entries will be ignored and will not appear in the
     * result map.
     *
     * If a key has no indexed value for the given [indexName], it will not appear in the
     * result map.
     *
     * @param timestamp The timestamp to execute the query on.
     * @param branch The branch to execute the query on.
     * @param keyspace The keyspace to query.
     * @param indexName The name of the index to query.
     *
     * @return A map from an index value to all keys in the given [keys] which are associated with this index value. Keys which do not exist in the
     * database or have no associated values in the given index will NOT appear in the values of this map.
     */
    fun getIndexedValues(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
    ): Map<Comparable<*>, Set<String>>

    /**
     * Scans the index for the values associated with the given keys.
     *
     * Keys that points to non-existing entries will be ignored and will not appear in the
     * result map.
     *
     * If a key has no indexed value for the given [indexName], it will not appear in the
     * result map.
     *
     * @param timestamp The timestamp to execute the query on.
     * @param branch The branch to execute the query on.
     * @param keyspace The keyspace to query.
     * @param indexName The name of the index to query.
     * @param keys The keys to look up. If empty, the result map will be empty. Keys that have no associated value in the database will be ignored.
     *
     * @return A map from an index value to all keys in the given [keys] which are associated with this index value. Keys which do not exist in the
     * database or have no associated values in the given index will NOT appear in the values of this map.
     */
    fun getIndexedValues(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
        keys: Set<String>
    ): Map<Comparable<*>, Set<String>>

    /**
     * Sorts the given [keys] using the given index.
     *
     * If a key does not appear in the database, it will be treated as if it had a `null` value associated to it.
     *
     * If a key is associated with multiple different values, the smallest of those values will be used for the
     * purpose of ascending sort, and the largest of those values will be used for descending sort.
     *
     * Example:
     *
     * ```
     *     Set<String> keys = getPrimaryKeysToSort();
     *     Branch master = db.getBranchManager().getMasterBranch();
     *     List<String> sortedKeys = db.getIndexManager().sortKeysWithIndex(
     *          master.getNow(),
     *          master,
     *          "default",
     *          keys,
     *          // assuming that "lastName" and "firstName" are valid & existing index names
     *          Sort.by("lastName", Order.ASCENDING, TextCompare.STRICT, NullSortOrder.NULLS_LAST).thenBy("firstName", Order.ASCENDING)
     *     )
     * ```
     *
     */
    fun sortKeysWithIndex(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        keys: Set<String>,
        sort: Sort
    ): List<String>

    // =====================================================================================================================
    // ROLLBACK METHODS
    // =====================================================================================================================

    /**
     * Performs a rollback on the index to the given timestamp.
     *
     * This affects all branches. Any index entries that belong to later timestamps will be removed.
     *
     * @param timestamp
     * The timestamp to roll back to. Must not be negative.
     *
     * @see .rollback
     * @see .rollback
     */
    fun rollback(timestamp: Long)

    /**
     * Performs a rollback on the index in the given branch to the given timestamp, for the given set of keys only.
     *
     * All index entries that are unrelated to the given keys will remain untouched by this operation, even if these
     * entries were added and/or modified after the given timestamp.
     *
     * @param branch
     * The branch to roll back. Must not be `null`.
     * @param timestamp
     * The timestamp to roll back to. Must not be negative.
     * @param keys
     * The keys to roll back. Must not be `null`. If this set is empty, this method returns
     * immediately and has no effect.
     *
     * @see .rollback
     */
    fun rollback(branch: Branch, timestamp: Long, keys: Set<QualifiedKey>)

    /**
     * Performs a rollback on the index in the given branch to the given timestamp.
     *
     * This operation will affect all entries in the given branch after the given timestamp.
     *
     * @param branch
     * The branch to roll back. Must not be `null`.
     * @param timestamp
     * The timestamp to roll back to. Must not be negative.
     *
     * @see .rollback
     * @see .rollback
     */
    fun rollback(branch: Branch, timestamp: Long)

    /**
     * Clears the internal query cache, if query result caching is enabled.
     *
     *
     * If query result caching is disabled, this method does nothing.
     */
    fun clearQueryCache()


}