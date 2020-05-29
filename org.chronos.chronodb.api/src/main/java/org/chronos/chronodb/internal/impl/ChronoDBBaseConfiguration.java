package org.chronos.chronodb.internal.impl;

import org.chronos.chronodb.api.CommitMetadataFilter;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.Comparison;
import org.chronos.common.configuration.ParameterValueConverters;
import org.chronos.common.configuration.annotation.*;
import org.chronos.common.logging.ChronoLogger;

import java.io.File;

@Namespace(ChronoDBConfiguration.NAMESPACE)
public abstract class ChronoDBBaseConfiguration extends AbstractConfiguration implements ChronoDBConfiguration {

    // =====================================================================================================================
    // DEFAULT SETTINGS
    // =====================================================================================================================

    private static final long DEFAULT__STORAGE_BACKEND_CACHE = 1024L * 1024L * 200L; // 200 MB (in bytes)

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    // general settings

    @Parameter(key = DEBUG)
    private boolean debugModeEnabled = false;

    @Parameter(key = MBEANS_ENABLED)
    private boolean mbeansEnabled = true;

    @EnumFactoryMethod("fromString")
    @Parameter(key = STORAGE_BACKEND)
    private String backendType;

    @Parameter(key = STORAGE_BACKEND_CACHE)
    @IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "JDBC")
    @IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "FILE")
    @IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "INMEMORY")
    private long storageBackendCacheMaxSize = DEFAULT__STORAGE_BACKEND_CACHE;

    @Parameter(key = CACHING_ENABLED)
    private boolean cachingEnabled = false;

    @Parameter(key = CACHE_MAX_SIZE)
    @RequiredIf(field = "cachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
    private Integer cacheMaxSize;

    @Parameter(key = QUERY_CACHE_ENABLED)
    private boolean indexQueryCachingEnabled = false;

    @Parameter(key = QUERY_CACHE_MAX_SIZE)
    @RequiredIf(field = "indexQueryCachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
    private Integer indexQueryCacheMaxSize;

    @Parameter(key = ASSUME_CACHE_VALUES_ARE_IMMUTABLE)
    @RequiredIf(field = "cachingEnabled", comparison = Comparison.IS_SET_TO, compareValue = "true")
    private boolean assumeCachedValuesAreImmutable = false;

    @Parameter(key = COMMIT_CONFLICT_RESOLUTION_STRATEGY, optional = true)
    private String conflictResolutionStrategyName;

    @EnumFactoryMethod("fromString")
    @Parameter(key = DUPLICATE_VERSION_ELIMINATION_MODE, optional = true)
    private DuplicateVersionEliminationMode duplicateVersionEliminationMode = DuplicateVersionEliminationMode.ON_COMMIT;

    @Parameter(key = COMMIT_METADATA_FILTER_CLASS, optional = true)
    private String commitMetadataFilterClassName = null;

    @Parameter(key = PERFORMANCE_LOGGING_FOR_COMMITS, optional = true)
    private boolean isCommitPerformanceLoggingActive = false;

    @Parameter(key = READONLY, optional = true)
    @IgnoredIf(field = "backendType", comparison = Comparison.IS_SET_TO, compareValue = "inmemory")
    private boolean readOnly = false;

    // =================================================================================================================
    // CACHES
    // =================================================================================================================

    private transient ConflictResolutionStrategy conflictResolutionStrategy;

    // =================================================================================================================
    // GENERAL SETTINGS
    // =================================================================================================================

    @Override
    public boolean isDebugModeEnabled() {
        return this.debugModeEnabled;
    }

    @Override
    public boolean isMBeanIntegrationEnabled() {
        return this.mbeansEnabled;
    }

    @Override
    public String getBackendType() {
        return this.backendType;
    }

    @Override
    public long getStorageBackendCacheMaxSize() {
        return this.storageBackendCacheMaxSize;
    }

    @Override
    public boolean isCachingEnabled() {
        return this.cachingEnabled;
    }

    @Override
    public Integer getCacheMaxSize() {
        return this.cacheMaxSize;
    }

    @Override
    public boolean isIndexQueryCachingEnabled() {
        return this.indexQueryCachingEnabled;
    }

    @Override
    public Integer getIndexQueryCacheMaxSize() {
        return this.indexQueryCacheMaxSize;
    }

    @Override
    public boolean isAssumeCachedValuesAreImmutable() {
        return this.assumeCachedValuesAreImmutable;
    }

    @Override
    public ConflictResolutionStrategy getConflictResolutionStrategy() {
        if (this.conflictResolutionStrategy == null) {
            // setting was not yet resolved, do it now
            this.conflictResolutionStrategy = ConflictResolutionStrategyLoader
                .load(this.conflictResolutionStrategyName);
            // we already resolved this setting, use the cached instance
        }
        return this.conflictResolutionStrategy;
    }

    @Override
    public DuplicateVersionEliminationMode getDuplicateVersionEliminationMode() {
        return this.duplicateVersionEliminationMode;
    }

    @Override
    public Class<? extends CommitMetadataFilter> getCommitMetadataFilterClass() {
        if (this.commitMetadataFilterClassName == null || this.commitMetadataFilterClassName.trim().isEmpty()) {
            return null;
        }
        try {
            return (Class<? extends CommitMetadataFilter>) Class.forName(this.commitMetadataFilterClassName.trim());
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            ChronoLogger.logWarning("Configuration warning: could not find Commit Metadata Filter class: '" + this.commitMetadataFilterClassName.trim() + "' (" + e.getClass().getSimpleName() + ")! No filter will be instantiated.");
            return null;
        }
    }

    @Override
    public boolean isCommitPerformanceLoggingActive() {
        return isCommitPerformanceLoggingActive;
    }

    @Override
    public boolean isReadOnly() {
        return this.readOnly;
    }

}
