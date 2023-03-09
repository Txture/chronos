package org.chronos.chronodb.internal.impl.index

import com.google.common.base.Preconditions
import com.google.common.collect.Sets
import org.chronos.chronodb.api.*
import org.chronos.chronodb.api.exceptions.*
import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.api.kotlin.ReadWriteAutoLockableExtensions.withNonExclusiveLock
import org.chronos.chronodb.internal.api.BranchEventListener
import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.chronodb.internal.api.ChronoDBInternal
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.index.IndexManagerInternal
import org.chronos.chronodb.internal.api.query.ChronoDBQuery
import org.chronos.chronodb.internal.api.query.searchspec.*
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.reduceValuesToLargestComparable
import org.chronos.chronodb.internal.impl.index.IndexManagerUtils.reduceValuesToSmallestComparable
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IteratorWrappingIndexScanCursor
import org.chronos.chronodb.internal.impl.index.diff.IndexingUtils
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache
import org.chronos.chronodb.internal.impl.index.querycache.LRUIndexQueryCache
import org.chronos.chronodb.internal.impl.index.querycache.NoIndexQueryCache
import org.chronos.chronodb.internal.impl.index.setview.SetView
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryOperatorElement
import org.chronos.chronodb.internal.impl.query.parser.ast.BinaryQueryOperator
import org.chronos.chronodb.internal.impl.query.parser.ast.QueryElement
import org.chronos.chronodb.internal.impl.query.parser.ast.WhereElement
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

abstract class AbstractIndexManager<C : ChronoDBInternal> : IndexManagerInternal {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    protected val queryCache: ChronoIndexQueryCache
    protected val owningDB: C

    // the index lock is a read-write lock, which should be used as follows:
    // - READ: for querying the index data, as well as meta-information (list all indices etc.)
    // - WRITE: for updating the dirty status, for adding/removing an index. NOT to be held for the entire duration of a reindexing operation (this would block all reads!)
    protected val indexLock: ReadWriteLock

    // the index management lock works in addition to the index lock and has to be held for ALL index management
    // operations, for the ENTIRE duration of the operations (e.g. add index, remove index, terminate index, reindex, ...).
    // We use an Object and "synchronized" here instead of a standard ReentrantLock, because the latter has
    // caused issues in the past on certain JVM implementations where the lock was acquired but never released.
    protected val indexManagementOperationLock: Any = Object()
    protected lateinit var indexTree: IndexTree

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: C) {
        this.owningDB = owningDB
        this.indexLock = ReentrantReadWriteLock(true)
        // check configuration to see if we want to have query caching
        val chronoDbConfig: ChronoDBConfiguration = this.owningDB.configuration
        queryCache = if (chronoDbConfig.isIndexQueryCachingEnabled) {
            val maxIndexQueryCacheSize = chronoDbConfig.indexQueryCacheMaxSize
            val debugModeEnabled = chronoDbConfig.isDebugModeEnabled
            LRUIndexQueryCache(maxIndexQueryCacheSize, debugModeEnabled)
        } else {
            // according to the configuration, no caching is required. To make sure that we still have
            // the same object structure (i.e. we don't have to deal with the cache object being NULL),
            // we create a pseudo-cache instead that actually "caches" nothing.
            NoIndexQueryCache()
        }
    }

    protected fun initializeIndicesFromDisk() {
        val branchManager = this.owningDB.branchManager
        val loadedIndices = this.loadIndicesFromPersistence()
        this.indexTree = IndexTree.create(branchManager, loadedIndices)
        branchManager.addBranchEventListener(object : BranchEventListener {

            override fun onBranchCreated(branch: Branch) {
                val indexChanges = this@AbstractIndexManager.indexTree.onBranchCreated(branch)
                this@AbstractIndexManager.applyIndexChangesToBackend(indexChanges)
            }

            override fun onBranchDeleted(branch: Branch) {
                val indexChanges = this@AbstractIndexManager.indexTree.onBranchDeleted(branch)
                this@AbstractIndexManager.applyIndexChangesToBackend(indexChanges)
            }

        })
    }

    // =================================================================================================================
    // INDEX QUERY METHODS
    // =================================================================================================================

    override fun queryIndex(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        searchSpec: SearchSpecification<*, *>,
    ): Set<String> {
        val indices = this.getIndices(branch, timestamp)
        this.assertIndexAccessIsOk(branch, timestamp, searchSpec, indices)
        return queryCache.getOrCalculate(timestamp, branch, keyspace, searchSpec) {
            this.performIndexQuery(timestamp, branch, keyspace, searchSpec)
        }
    }

    override fun evaluate(timestamp: Long, branch: Branch, query: ChronoDBQuery): Iterator<QualifiedKey> {
        Preconditions.checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must be >= 0!")
        Preconditions.checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!")
        return this.withLocksForIndexRead {
            // walk the AST of the query in a bottom-up fashion, applying the following strategy:
            // - WHERE node: run the query and remember the result set
            // - AND node: perform set intersection of left and right child result sets
            // - OR node: perform set union of left and right child result sets
            val keyspace = query.keyspace
            val rootElement = query.rootElement
            val resultSet = this.evaluateRecursive(rootElement, timestamp, branch, keyspace)
            resultSet.asSequence()
                .map { QualifiedKey.create(keyspace, it) }
                .iterator()
        }
    }

    override fun evaluateCount(timestamp: Long, branch: Branch, query: ChronoDBQuery): Long {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' (value: $timestamp) must be >= 0!" }
        return this.withLocksForIndexRead {
            // TODO PERFORMANCE: evaluating everything and then counting is not very efficient...
            val keyspace = query.keyspace
            val rootElement = query.rootElement
            val resultSet: Set<String> = this.evaluateRecursive(rootElement, timestamp, branch, keyspace)
            resultSet.size.toLong()
        }
    }

    override fun getIndices(): Set<SecondaryIndex> {
        return this.withLocksForIndexRead {
            this.indexTree.getAllIndices()
        }
    }

    override fun getIndexById(id: String): SecondaryIndex? {
        return this.withLocksForIndexRead {
            this.indexTree.getIndexById(id)
        }
    }

    override fun getIndices(branch: Branch): Set<SecondaryIndex> {
        return this.withLocksForIndexRead {
            this.indexTree.getIndices(branch)
        }
    }

    override fun getIndices(branch: Branch, timestamp: Long): Set<SecondaryIndex> {
        return this.withLocksForIndexRead {
            val actualBranch = this.owningDB.branchManager.getActualBranchForQuerying(branch, timestamp)
            this.indexTree.getIndices(actualBranch, timestamp)
        }
    }

    override fun getParentIndexOnBranch(index: SecondaryIndex, branch: Branch): SecondaryIndex {
        return this.withLocksForIndexRead {
            this.indexTree.getParentIndexOnBranch(index, branch)
        }
    }

    /**
     * Returns the parent indices of the given index.
     *
     * The order is as follows:
     *
     * - The given index (if [includeSelf] is `true`)
     * - The parent of the given index
     * - The parent of the parent of the given index
     * - ...
     *
     * If the given index has no parent, and [includeSelf] is `false`, the empty list will be returned.
     * If the given index has no parent, and [includeSelf] is `true`, the list will only contain the given index.
     *
     * @param index The index to get the parents for.
     * @param includeSelf Whether to include the given index in the result list or not.
     *
     * @return The list of parent indices, as outlined above.
     */
    override fun getParentIndicesRecursive(index: SecondaryIndex, includeSelf: Boolean): List<SecondaryIndex> {
        return this.withLocksForIndexRead {
            this.indexTree.getParentIndicesRecursive(index, includeSelf)
        }
    }

    override fun getDirtyIndices(): Set<SecondaryIndex> {
        return this.withLocksForIndexRead {
            this.indexTree.getAllIndices().asSequence().filter { it.dirty }.toSet()
        }
    }

    override fun isReindexingRequired(): Boolean {
        return this.withLocksForIndexRead {
            this.indexTree.getAllIndices().any { it.dirty }
        }
    }

    override fun deleteIndex(index: SecondaryIndex): Boolean {
        return this.deleteIndices(setOf(index))
    }

    override fun deleteIndices(indices: Set<SecondaryIndex>): Boolean {
        this.owningDB.configuration.assertNotReadOnly()
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                var changed = false
                for (index in indices) {
                    if (this.indexTree.getIndexById(index.id) == null) {
                        // this index doesn't exist
                        continue
                    }
                    val indexTreeChanges = this.indexTree.removeIndex(index)
                    this.applyIndexChangesToBackend(indexTreeChanges)
                    changed = true
                }
                if (changed) {
                    this.clearQueryCache()
                }
                changed
            }
        }
    }

    override fun clearAllIndices(): Boolean {
        this.owningDB.configuration.assertNotReadOnly()
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                if (this.indexTree.getAllIndices().isEmpty()) {
                    return@withLocksForIndexModification false
                }
                this.deleteAllIndicesInternal()
                this.indexTree.clear()
                clearQueryCache()
                return@withLocksForIndexModification true
            }
        }
    }

    override fun setIndexEndDate(index: SecondaryIndex, endTimestamp: Long): SecondaryIndex {
        this.owningDB.configuration.assertNotReadOnly()
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                val indexToModify = this.indexTree.getIndexById(index.id) as SecondaryIndexImpl?
                requireNotNull(indexToModify) {
                    "The given index ($index) does not exist! Maybe it was removed?"
                }
                require(endTimestamp > indexToModify.validPeriod.lowerBound) {
                    "The given end timestamp ($endTimestamp) for index $indexToModify is " +
                        "less than or equal to the start timestamp (${indexToModify.validPeriod.lowerBound}"
                }
                val indexWasDirty = indexToModify.dirty
                val oldUpperBound = indexToModify.validPeriod.upperBound

                val indexTreeChanges = this.indexTree.changeValidityPeriodUpperBound(indexToModify, endTimestamp)
                applyIndexChangesToBackend(indexTreeChanges)

                if (!indexWasDirty) {
                    // optimization: handle the case of modifying a clean index without reindexing everything
                    if (oldUpperBound > endTimestamp) {
                        // index shrink
                        val now = this.owningDB.tx(indexToModify.branch).timestamp
                        if (now > endTimestamp) {
                            this.rollback(indexToModify, endTimestamp)
                        }
                        // the index is now clean
                        indexToModify.dirty = false
                        this.saveIndexInternal(indexToModify)
                    }
                    // if the index validity was extended we always reindex
                }
                indexToModify
            }
        }
    }

    override fun addIndex(
        indexName: String,
        branch: String,
        startTimestamp: Long,
        endTimestamp: Long,
        indexer: Indexer<*>,
        options: Collection<IndexingOption>,
    ): SecondaryIndex {
        val actualBranch = this.owningDB.branchManager.getBranch(branch)
            ?: throw IllegalArgumentException("There is no branch named '${branch}'!")
        return this.addIndex(
            indexName,
            actualBranch,
            startTimestamp,
            endTimestamp,
            indexer,
            options
        )
    }

    override fun addIndex(
        indexName: String,
        branch: Branch,
        startTimestamp: Long,
        endTimestamp: Long,
        indexer: Indexer<*>,
        options: Collection<IndexingOption>,
    ): SecondaryIndex {
        this.owningDB.configuration.assertNotReadOnly()
        IndexingUtils.assertIsValidIndexName(indexName)
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                // check the other indexers on this index and make sure that indexer types cannot be mixed
                // on the same index name.

                val now = this.owningDB.tx(branch.name).timestamp
                require(System.currentTimeMillis() >= startTimestamp) {
                    "Can not create indices in the future! " +
                        "Argument 'validFrom' is $startTimestamp, now on branch ${branch.name} is $now"
                }
                val indexStartsDirty = when {
                    options.contains(StandardIndexingOption.ASSUME_NO_PRIOR_VALUES) -> false
                    else -> true
                }
                val index = SecondaryIndexImpl(
                    name = indexName,
                    id = UUID.randomUUID().toString(),
                    indexer = indexer,
                    validPeriod = Period.createRange(startTimestamp, endTimestamp),
                    branch = branch.name,
                    parentIndexId = null,
                    dirty = indexStartsDirty,
                    options = options.toSet()
                )
                this.assertHashCodeAndEqualsAreImplemented(indexName, indexer)
                val indexTreeChanges = this.indexTree.addIndex(index)
                this.applyIndexChangesToBackend(indexTreeChanges)
                clearQueryCache()
                index
            }
        }
    }

    override fun addIndices(indices: Set<SecondaryIndex>) {
        // don't trust those indices, they are coming from a dump
        this.owningDB.configuration.assertNotReadOnly()
        this.withIndexManagementLock {
            this.withLocksForIndexModification {
                val intersection = Sets.intersection(this.indexTree.getAllIndices(), indices)
                if (intersection.isNotEmpty()) {
                    val id = intersection.first().id
                    throw IllegalArgumentException("A SecondaryIndex with id '$id' already exists!")
                }
                val indexIds = mutableSetOf<String>()
                indexIds.addAll(this.indexTree.getAllIndices().asSequence().map { it.id })
                indexIds.addAll(indices.asSequence().map { it.id })

                val parentIds = mutableSetOf<String>()
                parentIds.addAll(this.indexTree.getAllIndices().asSequence().mapNotNull { it.parentIndexId })
                parentIds.addAll(indices.asSequence().mapNotNull { it.parentIndexId })

                for (parentId in parentIds) {
                    if (parentId !in indexIds) {
                        throw IllegalArgumentException("The index parent id '$parentId' does not refer to any known index!")
                    }
                }

                for (index in indices) {
                    this.assertNoOverlaps(index)
                    if (this.owningDB.branchManager.getBranch(index.branch) == null) {
                        throw IllegalArgumentException(
                            "The index $index belongs to branch ${index.branch}, " +
                                "but there is no branch with this name!"
                        )
                    }
                    this.assertHashCodeAndEqualsAreImplemented(index.name, index.indexer)
                }

                var indexTreeChanges = IndexChanges.EMPTY
                for (index in indices) {
                    indexTreeChanges = indexTreeChanges.addAll(indexTree.addIndex(index))
                }
                this.applyIndexChangesToBackend(indexTreeChanges)
                clearQueryCache()
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun markAllIndicesAsDirty(): Boolean {
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                val cleanIndices = this.indexTree.getAllIndices().asSequence().filterNot { it.dirty }.toSet()
                if (cleanIndices.isEmpty()) {
                    return@withLocksForIndexModification false
                }
                for (index in cleanIndices) {
                    (index as SecondaryIndexImpl).dirty = true
                }
                this.saveIndicesInternal(cleanIndices as Set<SecondaryIndexImpl>)
                return@withLocksForIndexModification true
            }
        }
    }

    override fun markIndicesAsDirty(indices: Collection<SecondaryIndex>): Boolean {
        return this.withIndexManagementLock {
            this.withLocksForIndexModification {
                val internalIndices = indices.asSequence()
                    .mapNotNull { this.indexTree.getIndexById(it.id) }
                    .filterIsInstance<SecondaryIndexImpl>()
                    .toSet()
                val modified = mutableSetOf<SecondaryIndexImpl>()
                for (index in internalIndices) {
                    if (index.dirty) {
                        index.dirty = true
                        modified += index
                    }
                }
                if (modified.isEmpty()) {
                    return@withLocksForIndexModification false
                }
                this.saveIndicesInternal(modified)
                true
            }
        }
    }


    override fun getIndexedValues(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
    ): Map<Comparable<*>, Set<String>> {
        return this.withLocksForIndexRead {
            this.createCursor<Comparable<Comparable<*>>>(
                timestamp = timestamp,
                branch = branch,
                keyspace = keyspace,
                indexName = indexName,
                sortOrder = Order.ASCENDING,
                textCompare = TextCompare.DEFAULT,
                keys = null
            ).use { cursor ->
                val resultMap = mutableMapOf<Comparable<*>, MutableSet<String>>()
                while (cursor.next()) {
                    resultMap.getOrPut(cursor.indexValue, ::mutableSetOf).add(cursor.primaryKey)
                }
                return@use resultMap
            }
        }
    }

    override fun getIndexedValues(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
        keys: Set<String>,
    ): Map<Comparable<*>, Set<String>> {
        if (keys.isEmpty()) {
            return emptyMap()
        }
        return this.withLocksForIndexRead {
            this.createCursor<Comparable<Comparable<*>>>(
                timestamp = timestamp,
                branch = branch,
                keyspace = keyspace,
                indexName = indexName,
                sortOrder = Order.ASCENDING,
                textCompare = TextCompare.DEFAULT,
                keys = keys
            ).use { cursor ->
                val resultMap = mutableMapOf<Comparable<*>, MutableSet<String>>()
                while (cursor.next()) {
                    resultMap.getOrPut(cursor.indexValue, ::mutableSetOf).add(cursor.primaryKey)
                }
                return@withLocksForIndexRead resultMap
            }
        }
    }

    override fun getIndexedValuesByKey(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
        keys: Set<String>,
    ): Map<String, Set<Comparable<*>>> {
        if (keys.isEmpty()) {
            return emptyMap()
        }
        return this.withLocksForIndexRead {
            this.createCursor<Comparable<Comparable<*>>>(
                timestamp = timestamp,
                branch = branch,
                keyspace = keyspace,
                indexName = indexName,
                sortOrder = Order.ASCENDING,
                textCompare = TextCompare.DEFAULT,
                keys = keys
            ).use { cursor ->
                val resultMap = mutableMapOf<String, MutableSet<Comparable<*>>>()
                while (cursor.next()) {
                    resultMap.getOrPut(cursor.primaryKey, ::mutableSetOf).add(cursor.indexValue)
                }
                return@withLocksForIndexRead resultMap
            }
        }
    }

    override fun getIndexedValuesByKey(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
    ): Map<String, Set<Comparable<*>>> {
        return this.withLocksForIndexRead {
            this.createCursor<Comparable<Comparable<*>>>(
                timestamp = timestamp,
                branch = branch,
                keyspace = keyspace,
                indexName = indexName,
                sortOrder = Order.ASCENDING,
                keys = null,
                textCompare = TextCompare.DEFAULT
            ).use { cursor ->
                val resultMap = mutableMapOf<String, MutableSet<Comparable<*>>>()
                while (cursor.next()) {
                    resultMap.getOrPut(cursor.primaryKey, ::mutableSetOf).add(cursor.indexValue)
                }
                return@withLocksForIndexRead resultMap
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <V : Comparable<V>> createCursor(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        indexName: String,
        sortOrder: Order,
        textCompare: TextCompare,
        keys: Set<String>?,
    ): IndexScanCursor<V> {
        val actualBranch = this.owningDB.branchManager.getActualBranchForQuerying(branch, timestamp)

        val index = this.indexTree.getIndices(actualBranch, timestamp).firstOrNull { it.name == indexName }
            ?: throw IllegalArgumentException("There is no index named '${indexName}' on branch '${actualBranch.name}' at timestamp ${timestamp}!")

        if (index.dirty || (keys != null && keys.size < 100)) {
            // fall back to primary index
            val tx = this.owningDB.tx(actualBranch.name, timestamp)
            val allKeys = keys ?: tx.keySet(keyspace)
            val iterator = allKeys.asSequence()
                .flatMap { key ->
                    val value = tx.get<Any?>(keyspace, key)
                    val indexValues = index.getIndexedValuesForObject(value)
                    if (indexValues.isEmpty()) {
                        return@flatMap emptySequence()
                    }
                    indexValues.asSequence().map { textCompare.apply(it as V) to key }
                }.let { seq ->
                    when (sortOrder) {
                        Order.ASCENDING -> seq.sortedBy { it.first }
                        Order.DESCENDING -> seq.sortedByDescending { it.first }
                    }
                }
                .iterator()
            return IteratorWrappingIndexScanCursor(iterator, sortOrder)
        }

        return createCursorInternal(actualBranch, timestamp, index, keyspace, indexName, sortOrder, textCompare, keys) as IndexScanCursor<V>
    }

    protected abstract fun createCursorInternal(
        branch: Branch,
        timestamp: Long,
        index: SecondaryIndex,
        keyspace: String,
        indexName: String,
        sortOrder: Order,
        textCompare: TextCompare,
        keys: Set<String>?,
    ): IndexScanCursor<*>

    override fun sortKeysWithIndex(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        keys: Set<String>,
        sort: Sort,
    ): List<String> {
        if (keys.isEmpty()) {
            return emptyList()
        }
        if (keys.size == 1) {
            return keys.toList()
        }
        return this.withLocksForIndexModification {
            val actualBranch = this.owningDB.branchManager.getActualBranchForQuerying(branch, timestamp)
            val indicesByName = this.getIndices(actualBranch, timestamp).associateBy { it.name }
            val indicesInOrder = sort.getIndexNamesInOrder().asSequence().map {
                indicesByName[it]
                    ?: throw IllegalArgumentException("The index '${it}' is unknown!")
            }.toList()

            if (indicesInOrder.size == 1) {
                // if we have only one index to sort by, we can utilize the sort order of the index itself.
                val index = indicesInOrder.single()
                val sortOrder = sort.getSortOrderForIndex(index.name)
                val textCompare = sort.getTextCompareForIndex(index.name)
                val nullSortPosition = sort.getNullSortPositionForIndex(index.name)
                return@withLocksForIndexModification sortKeysWithSingleSecondaryIndex(timestamp, actualBranch, keyspace, index, sortOrder, textCompare, nullSortPosition, keys)
            }

            return@withLocksForIndexModification sortKeysByMultipleSecondaryIndices(keys, indicesInOrder, sort, timestamp, actualBranch, keyspace)
        }
    }

    /**
     * Sorts the given keys by using a single secondary index.
     *
     * This is an optimized version of [sortKeysByMultipleSecondaryIndices] for the
     * (common) special case that only a single sort criterion is given, and ties are
     * broken via primary keys.
     */
    private fun sortKeysWithSingleSecondaryIndex(
        timestamp: Long,
        branch: Branch,
        keyspace: String,
        index: SecondaryIndex,
        sortOrder: Order,
        textCompare: TextCompare,
        nullSortPosition: NullSortPosition,
        keys: Set<String>,
    ): List<String> {
        this.createCursor<Comparable<Comparable<*>>>(timestamp, branch, keyspace, index.name, sortOrder, textCompare, keys).use { cursor ->
            val resultList = mutableListOf<String>()
            val unsortedKeys = keys.toMutableSet()
            while (cursor.next()) {
                val primaryKey = cursor.primaryKey
                if (primaryKey in unsortedKeys) {
                    resultList.add(primaryKey)
                    unsortedKeys.remove(primaryKey)
                }
            }
            // the remaining unsorted keys have NULL values. Check where
            // those values need to go in the ordering.
            val sortedNullKeys = unsortedKeys.sorted()
            when (nullSortPosition) {
                NullSortPosition.NULLS_FIRST -> resultList.addAll(0, sortedNullKeys)
                NullSortPosition.NULLS_LAST -> resultList.addAll(sortedNullKeys)
            }
            return resultList
        }
    }

    /**
     * Sorts the given keys by using multiple secondary indices.
     */
    private fun sortKeysByMultipleSecondaryIndices(
        keys: Set<String>,
        indicesInOrder: List<SecondaryIndex>,
        sort: Sort,
        timestamp: Long,
        branch: Branch,
        keyspace: String,
    ): List<String> {
        // start with ALL keys provided by the caller.
        var keysToCheck = keys
        // start with the first index
        val indexIterator = indicesInOrder.iterator()
        val keyToIndexValueList = mutableListOf<Map<String, Comparable<*>>>()
        while (keysToCheck.isNotEmpty() && indexIterator.hasNext()) {
            val currentIndex = indexIterator.next()
            val keyToIndexedValues: Map<String, Set<Comparable<*>>> = when (sort.getTextCompareForIndex(currentIndex.name)) {
                TextCompare.STRICT -> this.getIndexedValuesByKey(timestamp, branch, keyspace, currentIndex.name, keys)
                TextCompare.CASE_INSENSITIVE -> this.getIndexedValuesByKey(timestamp, branch, keyspace, currentIndex.name, keys)
                    .asSequence().map { entry ->
                        entry.key to entry.value.asSequence().map { TextCompare.CASE_INSENSITIVE.apply(it) }.toSet()
                    }.toMap()
            }
            // reduce the value sets to their dominant member
            val keyToIndexedValue = when (sort.getSortOrderForIndex(currentIndex.name)) {
                Order.ASCENDING -> keyToIndexedValues.reduceValuesToSmallestComparable()
                Order.DESCENDING -> keyToIndexedValues.reduceValuesToLargestComparable()
            }
            // check for non-null duplicates in the values
            val valueToKeys = keyToIndexedValue.entries.groupBy(keySelector = { it.value }, valueTransform = { it.key })
            val keysThatRequireTieBreaking = valueToKeys.asSequence()
                // if a group of index keys has more than one member,
                // those index values share the same key.
                .filter { it.value.size > 1 }
                // for the groups with size > 1, keep their members.
                // those are the keys that will require tie-breaking.
                .flatMap { it.value }
                .toSet()
            keyToIndexValueList.add(keyToIndexedValue)
            keysToCheck = keysThatRequireTieBreaking
        }

        val sortOrders = indicesInOrder.map { sort.getSortOrderForIndex(it.name) }
        val nullSortPositions = indicesInOrder.map { sort.getNullSortPositionForIndex(it.name) }

        return keys.asSequence()
            .map { key -> key to keyToIndexValueList.map { it[key] } }
            .sortedWith(IndexValuesComparator(sortOrders, nullSortPositions))
            .map { it.first }
            .toList()
    }

    // =================================================================================================================
    // ROLLBACK METHODS
    // =================================================================================================================

    override fun clearQueryCache() {
        this.queryCache.clear()
    }

    override fun getIndexQueryCache(): ChronoIndexQueryCache {
        return this.queryCache
    }

    override fun rollback(timestamp: Long) {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' (value: $timestamp) must not be negative!" }
        this.owningDB.configuration.assertNotReadOnly()
        this.withLocksForIndexModification {
            val indices = this.indexTree.getAllIndices().asSequence()
                .filterNot { it.validPeriod.isBefore(timestamp) }
                .filterIsInstance<SecondaryIndexImpl>()
                .toSet()
            this.rollback(indices, timestamp)
            clearQueryCache()
        }
    }

    override fun rollback(branch: Branch, timestamp: Long) {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' (value: $timestamp) must not be negative!" }
        this.owningDB.configuration.assertNotReadOnly()
        this.withLocksForIndexModification {
            val indices = this.indexTree.getIndices(branch).asSequence()
                .filterNot { it.validPeriod.isBefore(timestamp) }
                .filterIsInstance<SecondaryIndexImpl>()
                .toSet()
            this.rollback(indices, timestamp)
            clearQueryCache()
        }
    }

    override fun rollback(branch: Branch, timestamp: Long, keys: Set<QualifiedKey>) {
        require(timestamp >= 0) { "Precondition violation - argument 'timestamp' (value: $timestamp) must not be negative!" }
        this.owningDB.configuration.assertNotReadOnly()
        this.withLocksForIndexModification {
            val indices = this.indexTree.getIndices(branch).asSequence()
                .filterNot { it.validPeriod.isBefore(timestamp) }
                .filterIsInstance<SecondaryIndexImpl>()
                .toSet()
            this.rollback(indices, timestamp, keys)
        }
    }

    // =================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =================================================================================================================

    protected abstract fun performIndexQuery(timestamp: Long, branch: Branch, keyspace: String, searchSpec: SearchSpecification<*, *>): Set<String>

    protected abstract fun loadIndicesFromPersistence(): Set<SecondaryIndexImpl>

    protected abstract fun deleteIndexInternal(index: SecondaryIndexImpl)

    protected abstract fun deleteAllIndicesInternal()

    protected abstract fun saveIndexInternal(index: SecondaryIndexImpl)

    protected abstract fun rollback(index: SecondaryIndexImpl, timestamp: Long)

    protected abstract fun rollback(indices: Set<SecondaryIndexImpl>, timestamp: Long)

    protected abstract fun rollback(indices: Set<SecondaryIndexImpl>, timestamp: Long, keys: Set<QualifiedKey>)

    protected abstract fun saveIndicesInternal(indices: Set<SecondaryIndexImpl>)

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun assertNoOverlaps(endTimestamp: Long, indexToModify: SecondaryIndexImpl) {
        val newPeriod = indexToModify.validPeriod.setUpperBound(endTimestamp)
        if (endTimestamp > indexToModify.validPeriod.upperBound) {
            // extend validity period -> check for conflicts
            val isConflicting = this.indexTree.getAllIndices().asSequence()
                .filterNot { it.id == indexToModify.id }
                .filter { it.name == indexToModify.name }
                .filter { it.branch == indexToModify.branch }
                .filter { it.validPeriod.overlaps(newPeriod) }
                .any()
            if (isConflicting) {
                throw IllegalArgumentException(
                    "Can not extend validity period of indexer $indexToModify to timestamp $endTimestamp, " +
                        "because it overlaps with the validity period of another index on the same property!"
                )
            }
        }
    }

    private fun assertNoOverlaps(newIndex: SecondaryIndex) {
        val isConflicting = this.indexTree.getAllIndices().asSequence()
            .filterNot { it.id == newIndex.id }
            .filter { it.name == newIndex.name }
            .filter { it.branch == newIndex.branch }
            .filter { it.validPeriod.overlaps(newIndex.validPeriod) }
            .any()
        if (isConflicting) {
            throw ChronoDBIndexingException(
                "Can not add index $newIndex " +
                    "because it overlaps with the validity period of another index on the same property!"
            )
        }

    }

    private fun assertHashCodeAndEqualsAreImplemented(indexName: String, indexerToAdd: Indexer<*>) {
        var foundHashCodeOverride = false
        var foundEqualsOverride = false
        var currentClass: Class<*>? = indexerToAdd.javaClass
        while (currentClass != null && currentClass != Any::class.java) {
            try {
                val equalsMethod = currentClass.getMethod("equals", Any::class.java)
                if (equalsMethod.declaringClass != Any::class.java) {
                    foundEqualsOverride = true
                }
            } catch (_: NoSuchMethodException) {
                // equals() is not overridden here, check parent class
            } catch (_: SecurityException) {
                // reflection not allowed, can't perform this check
                return
            }
            try {
                val hashCodeMethod = currentClass.getMethod("hashCode")
                if (hashCodeMethod.declaringClass != Any::class.java) {
                    foundHashCodeOverride = true
                }
            } catch (_: NoSuchMethodException) {
                // hashCode() is not overridden here, check parent class
            } catch (_: SecurityException) {
                // reflection not allowed, can't perform this check
                return
            }
            currentClass = currentClass.superclass
        }
        if (!foundHashCodeOverride || !foundEqualsOverride) {
            val indexerType = this.getIndexerType(indexerToAdd)
            throw ChronoDBIndexingException(
                "Cannot add ${indexerType.simpleName} '${indexerToAdd.javaClass.name}' to index '$indexName', " +
                    "because it does not implement hashCode() and/or equals(Object). " +
                    "Please provide suitable implementations in your indexer class."
            )
        }
        // do a VERY basic check on the functionality of the equals(...) method:
        // assert that the object is equal to itself.
        if (!Objects.equals(indexerToAdd, indexerToAdd)) {
            val indexerType = this.getIndexerType(indexerToAdd)
            throw ChronoDBIndexingException(
                "Cannot add ${indexerType.simpleName} '${indexerToAdd.javaClass.name}' to index '$indexName', " +
                    "because its equals(Object) method is broken - the indexer is not equal to itself. " +
                    "Please fix this in your indexer implementation."
            )
        }
    }

    private fun getIndexerType(indexer: Indexer<*>): Class<out Indexer<*>> {
        return when (indexer) {
            is StringIndexer -> StringIndexer::class.java
            is LongIndexer -> LongIndexer::class.java
            is DoubleIndexer -> DoubleIndexer::class.java
            else -> throw IllegalArgumentException("Unknown Indexer subclass: '${indexer.javaClass.name}'!")
        }
    }

    private fun evaluateRecursive(
        element: QueryElement,
        timestamp: Long,
        branch: Branch,
        keyspace: String,
    ): Set<String> {
        return when (element) {
            is BinaryOperatorElement -> {
                // disassemble the element
                val left = element.leftChild
                val right = element.rightChild
                val op = element.operator
                // recursively evaluate left and right child result sets
                val leftResult = evaluateRecursive(left, timestamp, branch, keyspace)
                val rightResult = evaluateRecursive(right, timestamp, branch, keyspace)
                val resultSet = when (op) {
                    BinaryQueryOperator.AND -> SetView.intersection(leftResult, rightResult)
                    BinaryQueryOperator.OR -> SetView.union(leftResult, rightResult)
                    // safeguard
                    null -> throw IllegalArgumentException("Binary operator is null!")
                }
                //  note: set views are always unmodifiable
                resultSet
            }

            is WhereElement<*, *> -> {
                // disassemble and execute the atomic query
                val actualBranch = this.owningDB.branchManager.getActualBranchForQuerying(branch, timestamp)
                val index = this.resolveIndex(actualBranch, timestamp, element.indexName)
                val searchSpec = element.toSearchSpecification(index)
                val keys = queryIndex(timestamp, actualBranch, keyspace, searchSpec)
                Collections.unmodifiableSet(keys)
            }

            else -> {
                // all other elements should be eliminated by optimizations...
                throw ChronoDBQuerySyntaxException(
                    "Query contains unsupported element of class '"
                        + element.javaClass.name + "' - was the query optimized?"
                )
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun applyIndexChangesToBackend(indexTreeChanges: IndexChanges) {
        this.saveIndicesInternal(indexTreeChanges.addedIndices as Set<SecondaryIndexImpl>)
        this.saveIndicesInternal(indexTreeChanges.updatedIndices as Set<SecondaryIndexImpl>)
        for (removedIndex in indexTreeChanges.removedIndices) {
            this.deleteIndexInternal(removedIndex as SecondaryIndexImpl)
        }
    }

    private fun resolveIndex(branch: Branch, timestamp: Long, indexName: String): SecondaryIndex {
        val index = this.getIndices(branch, timestamp).firstOrNull { it.name == indexName }
            ?: throw UnknownIndexException(
                "There is no index '${indexName}' on branch '${branch.name}' at timestamp ${timestamp}!"
            )
        if (index.dirty) {
            throw IllegalStateException(
                "The index '${indexName}' on branch '${branch.name}' at timestamp ${timestamp}" +
                    " is dirty and cannot be used! Call 'db.getIndexManager().reindexAll()'" +
                    " to perform re-indexing."
            )
        }
        return index
    }

    private fun assertIndexAccessIsOk(
        branch: Branch,
        timestamp: Long,
        searchSpec: SearchSpecification<*, *>,
        indices: Set<SecondaryIndex>,
    ) {
        val index = indices.singleOrNull { it.id == searchSpec.index.id }
            ?: throw UnknownIndexException(
                "There is no index with ID '${searchSpec.index.id}' on branch '${branch.name}'" +
                    " at timestamp $timestamp! Available indices are: " +
                    indices.joinToString()
            )

        if (!index.validPeriod.contains(timestamp)) {
            throw UnknownIndexException(
                "There is no index within valid period."
            )
        }
        val isStringIndex = index.valueType == String::class.java
        val isLongIndex = index.valueType == Long::class.java
        val isDoubleIndex = index.valueType == Double::class.java
        check(isStringIndex || isLongIndex || isDoubleIndex) {
            "Could not determine index type of index '$index'!"
        }
        when (searchSpec) {
            is ContainmentSearchSpecification<*> -> when {
                isStringIndex && searchSpec !is ContainmentStringSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access String index '" + index + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!")
                }

                isLongIndex && searchSpec !is ContainmentLongSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access Long index '" + index + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!")
                }

                isDoubleIndex && searchSpec !is ContainmentDoubleSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access Double index '" + index + "' with " + searchSpec.getDescriptiveSearchType() + " search [" + searchSpec + "]!")
                }
            }

            else -> when {
                isStringIndex && searchSpec !is StringSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access String index '" + index + "' with " + searchSpec.descriptiveSearchType + " search [" + searchSpec + "]!")
                }

                isLongIndex && searchSpec !is LongSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access Long index '" + index + "' with " + searchSpec.descriptiveSearchType + " search [" + searchSpec + "]!")
                }

                isDoubleIndex && searchSpec !is DoubleSearchSpecification -> {
                    throw InvalidIndexAccessException("Cannot access Double index '" + index + "' with " + searchSpec.descriptiveSearchType + " search [" + searchSpec + "]!")
                }
            }

        }
    }

    override fun <T> withIndexWriteLock(action: () -> T): T {
        return this.withLocksForIndexModification {
            try {
                action()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while modifying an index: ${e}", e)
            }
        }
    }

    override fun <T> withIndexReadLock(action: () -> T): T {
        return this.withLocksForIndexRead {
            try {
                action()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while reading an index: ${e}", e)
            }
        }
    }

    override fun <T> withIndexReadLock(action: Callable<T>): T {
        return this.withLocksForIndexRead {
            try {
                action.call()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while reading an index: ${e}", e)
            }
        }
    }

    override fun withIndexReadLock(action: Runnable) {
        return this.withLocksForIndexRead {
            try {
                action.run()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while reading an index: ${e}", e)
            }
        }
    }

    override fun <T> withIndexWriteLock(action: Callable<T>): T {
        return this.withLocksForIndexModification {
            try {
                action.call()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while modifying an index: ${e}", e)
            }
        }
    }

    override fun withIndexWriteLock(action: Runnable) {
        return this.withLocksForIndexModification {
            try {
                action.run()
            } catch (e: Exception) {
                throw ChronoDBException("An exception occurred while modifying an index: ${e}", e)
            }
        }
    }

    protected inline fun <T> withLocksForIndexModification(crossinline action: () -> T): T {
        // we generally want to permit reads while we delete indices (read lock)...
        return this.owningDB.withNonExclusiveLock {
            // ... but we have to be careful to make sure that those reads do not
            // use any index we're currently working on (write lock).
            this.indexLock.writeLock().withLock(action)
        }
    }

    protected inline fun <T> withLocksForIndexRead(crossinline action: () -> T): T {
        // we generally want to permit reads while we delete indices (read lock)...
        return this.owningDB.withNonExclusiveLock {
            // ... but we have to be careful to make sure that those
            // reads do not use this particular index (write lock).
            this.indexLock.readLock().withLock(action)
        }
    }

    protected inline fun <T> withIndexManagementLock(crossinline action: () -> T): T {
        synchronized(this.indexManagementOperationLock) {
            return action()
        }
    }
}