package org.chronos.chronodb.internal.api.cache;

import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.impl.cache.bogus.ChronoDBBogusCache;
import org.chronos.chronodb.internal.impl.cache.headfirst.HeadFirstCache;
import org.chronos.chronodb.internal.impl.cache.mosaic.MosaicCache;

import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

/**
 * The {@link ChronoDBCache} is responsible for caching {@link GetResult}s.
 *
 * <p>
 * This helps to reduce accesses to the underlying store, which in turn reduces the need for I/O operations.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoDBCache {

    // =====================================================================================================================
    // FACTORY METHODS
    // =====================================================================================================================

    /**
     * Creates a new cache according to the settings in the given configuration.
     *
     * @param config The configuration to read the cache settings from. Must not be <code>null</code>.
     * @return A cache instance matching the given configuration. Never <code>null</code>.
     */
    public static ChronoDBCache createCacheForConfiguration(final ChronoDBConfiguration config) {
        checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
        if (config.isCachingEnabled()) {
            switch (config.getCacheType()) {
                case MOSAIC:
                    return new MosaicCache(config.getCacheMaxSize());
                case HEAD_FIRST:
                    return new HeadFirstCache(
                        config.getCacheMaxSize(),
                        config.getCacheHeadFirstPreferredBranch(),
                        config.getCacheHeadFirstPreferredKeyspace()
                    );
            }
        }
        return new ChronoDBBogusCache();

    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    /**
     * Queries the cache for the entry with the given key at the given branch and timestamp.
     *
     * @param branch       The branch to search in. Must not be <code>null</code>.
     * @param timestamp    The timestamp to search at. Must not be <code>null</code>.
     * @param qualifiedKey The qualified key to search for. Must not be <code>null</code>.
     * @return The result. Never <code>null</code>. May be a {@linkplain CacheGetResult#isMiss() cache miss}.
     */
    public <T> CacheGetResult<T> get(String branch, long timestamp, QualifiedKey qualifiedKey);

    /**
     * Adds the given {@link GetResult} to this cache.
     *
     * @param branch      The branch that was requested. Must not be <code>null</code>.
     * @param queryResult The result of the query that should be cached. Must not be <code>null</code>.
     */
    public void cache(String branch, GetResult<?> queryResult);

    /**
     * Writes the given key-value pair through the cache.
     *
     * <p>
     * This is similar to {@link #cache(String, GetResult)}, except that this method works directly on the <code>value</code> object instead of a {@link GetResult}.
     *
     * <p>
     * This method assumes that the validity period of the given key-value pair is open-ended at the righthand side.
     *
     * @param branch    The branch of the entry to write through the cache.
     * @param timestamp The timestamp of the entry to write through the cache.
     * @param key       The qualified key of the key-value pair
     * @param value     The value of the key-value pair
     */
    public void writeThrough(String branch, long timestamp, QualifiedKey key, Object value);

    /**
     * Retrieves the current number of entries in the cache.
     *
     * @return The current size of the cache. Never negative, may be zero.
     */
    public int size();


    /**
     * Returns the maximum number of entries permitted in this cache.
     *
     * Implementations that do not have a hard limit should return -1.
     *
     * @return The maximum size of this cache.
     */
    public int maxSize();

    /**
     * Returns the statistics of this cache.
     *
     * <p>
     * This method will return a <b>copy</b> of the statistics. Any modifications on the cache (including {@link #resetStatistics()}) will have <b>no influence</b> on the returned statistics object.
     *
     * @return A copy of the statistics. Never <code>null</code>.
     */
    public CacheStatistics getStatistics();

    /**
     * Resets the statistics of this cache.
     */
    public void resetStatistics();

    /**
     * Completely clears the contents of this cache, eliminating all entries from it.
     */
    public void clear();

    /**
     * Rolls the cache back to the specified timestamp, i.e. removes all entries that are newer than the specified timestamp.
     *
     * @param timestamp The timestamp to roll back to. Must not be negative.
     */
    public void rollbackToTimestamp(long timestamp);

    /**
     * Writes the given map of key-value pairs through this cache i.e. adds all entries to the cache.
     *
     * <p>
     * This method assumes that the validity range of all pairs is open-ended on the righthand side.
     *
     * @param branch    The branch to associate the entries with. Must not be <code>null</code>.
     * @param timestamp The timestamp to associate the entries with. Must not be <code>null</code>.
     * @param keyValues The key-value pairs to write through the cache. May be empty, but must not be <code>null</code>.
     */
    public default void writeThrough(final String branch, final long timestamp,
                                     final Map<QualifiedKey, Object> keyValues) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(keyValues, "Precondition violation - argument 'keyValues' must not be NULL!");
        for (Entry<QualifiedKey, Object> entry : keyValues.entrySet()) {
            this.writeThrough(branch, timestamp, entry.getKey(), entry.getValue());
        }
    }

    // =====================================================================================================================
    // INNER INTERFACES
    // =====================================================================================================================

    /**
     * This interface describes a set of statistics that can be retrieved about a {@link ChronoDBCache}.
     *
     * <p>
     * Use {@link ChronoDBCache#getStatistics()} to retrieve an instance of this interface.
     *
     * @author martin.haeusler@uibk.ac.at -- Initial contribution and API
     */
    public interface CacheStatistics {

        /**
         * Returns the number of cache hits recorded in this cache.
         *
         * <p>
         * A cache hit is a request to {@link ChronoDBCache#get(String, long, QualifiedKey)} that returns a {@linkplain CacheGetResult result} that represents a {@linkplain CacheGetResult#isHit() hit}.
         *
         * @return The number of cache hits. Never negative, may be zero.
         */
        public long getCacheHitCount();

        /**
         * Returns the number of cache misses recorded in this cache.
         *
         * <p>
         * A cache miss is a request to {@link ChronoDBCache#get(String, long, QualifiedKey)} that returns a {@linkplain CacheGetResult result} that represents a {@linkplain CacheGetResult#isMiss() miss}.
         *
         * @return The number of cache misses. Never negative, may be zero.
         */
        public long getCacheMissCount();

        /**
         * Returns the number of elements that have been evicted from this cache due to limited population size.
         *
         * Evictions due to cache clearing or rollback do not count towards this number.
         *
         * Implementations without a hard limit on the population size should return 0.
         *
         * @return The number of elements removed from the cache due to the eviction policy.
         */
        public long getEvictionCount();

        /**
         * Returns the number of times a rollback has occurred on this cache.
         *
         * @return The number of times a rollback has occurred so far.
         */
        public long getRollbackCount();

        /**
         * Returns the number of times this cache has been explicitly cleared.
         *
         * @return The number of times this cache has been explicitly cleared.
         */
        public long getClearCount();

        /**
         * Returns the request count, i.e. the number of {@link ChronoDBCache#get(String, long, QualifiedKey)} calls received by this cache.
         *
         * @return The request count. May be zero, but never negative.
         */
        public default long getRequestCount() {
            return getCacheHitCount() + getCacheMissCount();
        }

        /**
         * Returns the cache hit ratio.
         *
         * <p>
         * The cache hit ratio is the {@link #getCacheHitCount()} divided by the {@link #getRequestCount()}.
         *
         * <p>
         * The sum of the {@linkplain #getCacheHitRatio() hit ratio} and the {@linkplain #getCacheMissRatio() miss ratio} is always equal to 1 (modulo precision errors), except when the {@linkplain #getRequestCount() request count} is zero.
         *
         * @return The cache hit count. Will be {@link Double#NaN NaN} if the {@linkplain #getRequestCount() request count} is zero.
         */
        public default double getCacheHitRatio() {
            long requestCount = this.getRequestCount();
            if (requestCount == 0) {
                // by definition, if we have no requests, we have no hit ratio
                return Double.NaN;
            }
            return ((double) getCacheHitCount()) / requestCount;
        }

        /**
         * Returns the cache miss ratio.
         *
         * <p>
         * The cache miss ratio is the {@link #getCacheMissCount()} divided by the {@link #getRequestCount()}.
         *
         * <p>
         * The sum of the {@linkplain #getCacheHitRatio() hit ratio} and the {@linkplain #getCacheMissRatio() miss ratio} is always equal to 1 (modulo precision errors), except when the {@linkplain #getRequestCount() request count} is zero.
         *
         * @return The cache miss count. Will be {@link Double#NaN NaN} if the {@linkplain #getRequestCount() request count} is zero.
         */
        public default double getCacheMissRatio() {
            long requestCount = this.getRequestCount();
            if (requestCount == 0) {
                // by definition, if we have no requests, we have no miss ratio
                return Double.NaN;
            }
            return ((double) getCacheMissCount()) / requestCount;
        }

    }

}
