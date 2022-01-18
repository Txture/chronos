package org.chronos.chronodb.internal.impl.cache.headfirst

import com.google.common.annotations.VisibleForTesting
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.internal.api.GetResult
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.cache.CacheGetResult
import org.chronos.chronodb.internal.api.cache.ChronoDBCache
import org.chronos.chronodb.internal.impl.cache.CacheStatisticsImpl
import java.util.*
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class HeadFirstCache(
    val maxSize: Int,
    val preferredBranch: String?,
    val preferredKeyspace: String?
) : ChronoDBCache {

    private val lock: ReadWriteLock = ReentrantReadWriteLock(true)
    private val cacheStatistics = CacheStatisticsImpl()

    /** Maps from Branch Name and QualifiedKey to CacheRow.*/
    private val contents = mutableMapOf<String, MutableMap<QualifiedKey, CacheRow>>()

    /** A helper data structure to realize the desired eviction order.*/
    private val evictionOrder: NavigableSet<EvictionOrderKey> = TreeSet(
        EvictionOrderKey.createEvictionOrderComparator(preferredBranch, preferredKeyspace)
    )

    private var size = 0

    override fun <T : Any?> get(branch: String, timestamp: Long, qualifiedKey: QualifiedKey): CacheGetResult<T> {
        this.lock.readLock().withLock {
            val cacheRow = this.getCacheRow(branch, qualifiedKey)
            if (cacheRow == null) {
                cacheStatistics.registerMiss()
                return CacheGetResult.miss()
            }
            val cacheGetResult = cacheRow.get<T>(timestamp)
            if (cacheGetResult.isHit) {
                cacheStatistics.registerHit()
            } else {
                cacheStatistics.registerMiss()
            }
            return cacheGetResult
        }
    }

    private fun getCacheRow(branch: String, qualifiedKey: QualifiedKey): CacheRow? {
        return this.contents[branch]?.get(qualifiedKey)
    }

    private fun getOrCreateCacheRow(branch: String, qualifiedKey: QualifiedKey): CacheRow {
        val map = this.contents.getOrPut(branch) { mutableMapOf() }
        return map.getOrPut(qualifiedKey) { CacheRow(qualifiedKey) }
    }

    override fun cache(branch: String, queryResult: GetResult<*>) {
        if (!queryResult.isHit) {
            // do not cache misses
            return
        }
        val evictionOrderKey = EvictionOrderKey(
            branch = branch,
            keyspace = queryResult.requestedKey.keyspace,
            key = queryResult.requestedKey.key,
            period = queryResult.period
        )
        this.lock.writeLock().withLock {
            val cacheRow = this.getOrCreateCacheRow(branch, queryResult.requestedKey)
            val added = cacheRow.put(queryResult)
            if (added) {
                this.size++
            }
            this.evictionOrder.add(evictionOrderKey)
            this.shrinkIfNecessary()
        }
    }

    override fun writeThrough(branch: String, timestamp: Long, key: QualifiedKey, value: Any?) {
        this.lock.writeLock().withLock {
            val cacheRow = this.getOrCreateCacheRow(branch, key)
            val existingEntry = cacheRow.get<Any?>(timestamp)
            cacheRow.writeThrough(timestamp, value)

            if (existingEntry.isHit) {
                val lowerBound = existingEntry.validFrom
                val existingEvictionOrderKey = EvictionOrderKey(
                    branch = branch,
                    keyspace = key.keyspace,
                    key = key.key,
                    period = Period.createOpenEndedRange(lowerBound)
                )
                this.evictionOrder.remove(existingEvictionOrderKey)
                val updatedEvictionOrderKey = EvictionOrderKey(
                    branch = branch,
                    keyspace = key.keyspace,
                    key = key.key,
                    period = Period.createRange(lowerBound, timestamp)
                )
                this.evictionOrder.add(updatedEvictionOrderKey)
            }

            val newEvictionOrderKey = EvictionOrderKey(
                branch = branch,
                keyspace = key.keyspace,
                key = key.key,
                period = Period.createOpenEndedRange(timestamp)
            )
            this.evictionOrder.add(newEvictionOrderKey)

            this.size++

            this.shrinkIfNecessary()
        }
    }

    override fun size(): Int {
        return this.lock.readLock().withLock {
            this.size
        }
    }

    override fun maxSize(): Int {
        return this.maxSize
    }

    @VisibleForTesting
    fun computedSize(): Int {
        this.lock.readLock().withLock {
            return this.contents.values.asSequence().flatMap { it.values }.sumOf { it.size() }
        }
    }

    override fun getStatistics(): ChronoDBCache.CacheStatistics {
        return cacheStatistics.duplicate()
    }

    override fun resetStatistics() {
        cacheStatistics.reset()
    }

    override fun clear() {
        this.lock.writeLock().withLock {
            // we register the clear event BEFORE doing it in case
            // we encounter an error during the process. Registering
            // the event first will at least make it show up in the
            // cache statistics.
            this.cacheStatistics.registerClear()
            contents.clear()
            evictionOrder.clear()
            this.size = 0
        }
    }

    override fun rollbackToTimestamp(timestamp: Long) {
        this.lock.writeLock().withLock {
            // we register the rollback event BEFORE doing it in case
            // we encounter an error during the process. Registering
            // the event first will at least make it show up in the
            // cache statistics.
            this.cacheStatistics.registerRollback()
            var removed = 0
            val branchesToRemove = mutableSetOf<String>()
            for ((branch, qKeyToRow) in this.contents) {
                val qKeysToRemove = mutableSetOf<QualifiedKey>()
                for (row in qKeyToRow.values) {
                    removed += row.rollbackToTimestamp(timestamp)
                    if (row.isEmpty()) {
                        qKeysToRemove += row.rowKey
                    }
                }
                qKeyToRow.removeAll(qKeysToRemove)
                if (qKeyToRow.isEmpty()) {
                    branchesToRemove.add(branch)
                }
            }
            this.contents.removeAll(branchesToRemove)
            this.size -= removed
            val iterator = this.evictionOrder.iterator()
            while (iterator.hasNext()) {
                val current = iterator.next()
                if (current.period.lowerBound >= timestamp || current.period.contains(timestamp)) {
                    iterator.remove()
                }
            }
        }
    }

    private fun shrinkIfNecessary() {
        if (this.size() <= maxSize) {
            return
        }
        val targetSize = this.maxSize
        val elementsToRemove = this.size() - targetSize
        val removedKeys = mutableListOf<EvictionOrderKey>()
        val iterator = this.evictionOrder.descendingIterator()
        while (iterator.hasNext() && removedKeys.size < elementsToRemove) {
            val current = iterator.next()
            iterator.remove()
            removedKeys.add(current)
        }
        val branchesToRemove = mutableSetOf<String>()
        for (removedKey in removedKeys) {
            val qKeyToRow = this.contents[removedKey.branch]
            if (qKeyToRow != null) {
                val qKeysToRemove = mutableSetOf<QualifiedKey>()
                val row = qKeyToRow[QualifiedKey.create(removedKey.keyspace, removedKey.key)]
                if (row != null) {
                    row.delete(removedKey.period.lowerBound)
                    if (row.isEmpty()) {
                        qKeysToRemove += row.rowKey
                    }
                    qKeyToRow.removeAll(qKeysToRemove)
                }

                if (qKeyToRow.isEmpty()) {
                    branchesToRemove.add(removedKey.branch)
                }
            }
        }
        this.contents.removeAll(branchesToRemove)
        this.size -= removedKeys.size
        this.cacheStatistics.registerEvictions(removedKeys.size.toLong())
    }

    private fun <K, V> MutableMap<K, V>.removeAll(keys: Iterable<K>): Int {
        var result = 0
        for (key in keys) {
            val removed = this.remove(key)
            if (removed != null) {
                result++
            }
        }
        return result
    }

    @VisibleForTesting
    fun checkIfDataStructuresAreClean(): Boolean {
        // we shouldn't have branch maps pointing to empty maps
        if (this.contents.any { it.value.isEmpty() }) {
            return false
        }
        // no mapping from Qualified Key to Row should point to an empty row
        if (this.contents.values.asSequence().flatMap { it.values }.any { it.isEmpty() }) {
            return false
        }
        return true
    }

}