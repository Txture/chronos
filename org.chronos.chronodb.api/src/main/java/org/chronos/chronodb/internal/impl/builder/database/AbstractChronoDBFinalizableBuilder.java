package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.CommitMetadataFilter;
import org.chronos.chronodb.api.DuplicateVersionEliminationMode;
import org.chronos.chronodb.api.builder.database.ChronoDBFinalizableBuilder;
import org.chronos.chronodb.api.conflict.ConflictResolutionStrategy;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBFactoryInternal;

public abstract class AbstractChronoDBFinalizableBuilder<SELF extends ChronoDBFinalizableBuilder<?>>
		extends AbstractChronoDBBuilder<SELF> implements ChronoDBFinalizableBuilder<SELF> {

	@Override
	@SuppressWarnings("unchecked")
	public SELF withLruCacheOfSize(final int maxSize) {
		if (maxSize > 0) {
			this.withProperty(ChronoDBConfiguration.CACHING_ENABLED, "true");
			this.withProperty(ChronoDBConfiguration.CACHE_MAX_SIZE, String.valueOf(maxSize));
		} else {
			this.withProperty(ChronoDBConfiguration.CACHING_ENABLED, "false");
		}
		return (SELF) this;
	}

	@Override
	public SELF assumeCachedValuesAreImmutable(final boolean value) {
		return this.withProperty(ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, String.valueOf(value));
	}

	@Override
	@SuppressWarnings("unchecked")
	public SELF withLruQueryCacheOfSize(final int maxSize) {
		if (maxSize > 0) {
			this.withProperty(ChronoDBConfiguration.QUERY_CACHE_ENABLED, "true");
			this.withProperty(ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, String.valueOf(maxSize));
		} else {
			this.withProperty(ChronoDBConfiguration.QUERY_CACHE_ENABLED, "false");
		}
		return (SELF) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public SELF withDuplicateVersionElimination(final boolean useDuplicateVersionElimination) {
		DuplicateVersionEliminationMode mode = DuplicateVersionEliminationMode.DISABLED;
		if (useDuplicateVersionElimination) {
			mode = DuplicateVersionEliminationMode.ON_COMMIT;
		}
		return this.withProperty(ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, mode.toString());
	}

	@Override
	@SuppressWarnings("unchecked")
	public SELF withConflictResolutionStrategy(final ConflictResolutionStrategy strategy) {
		checkNotNull(strategy, "Precondition violation - argument 'strategy' must not be NULL!");
		return this.withProperty(ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, strategy.getClass().getName());
	}

	@Override
	public SELF withCommitMetadataFilter(final Class<? extends CommitMetadataFilter> filterClass) {
		if(filterClass == null){
			return this.withProperty(ChronoDBConfiguration.COMMIT_METADATA_FILTER_CLASS, "");
		}else{
			return this.withProperty(ChronoDBConfiguration.COMMIT_METADATA_FILTER_CLASS, filterClass.getName());
		}
	}

	public SELF withPerformanceLoggingForCommits(final boolean enablePerformanceLoggingForCommits){
		return this.withProperty(ChronoDBConfiguration.PERFORMANCE_LOGGING_FOR_COMMITS, String.valueOf(enablePerformanceLoggingForCommits));
	}

	@Override
	public ChronoDB build() {
		return ChronoDBFactoryInternal.INSTANCE.create(this.getConfiguration());
	}

}
