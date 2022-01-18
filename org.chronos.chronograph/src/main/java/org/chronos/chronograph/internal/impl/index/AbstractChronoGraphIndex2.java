package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;

import java.util.Collections;
import java.util.Set;

/**
 * @deprecated  Use {@link ChronoGraphIndex3} instead. This class only exists for backwards compatibility.
 */
@Deprecated
public abstract class AbstractChronoGraphIndex2 implements ChronoGraphIndexInternal {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	protected String indexedProperty;
	protected IndexType indexType;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected AbstractChronoGraphIndex2() {
		// default constructor for serialization
	}

	public AbstractChronoGraphIndex2(final String indexedProperty, final IndexType indexType) {
		checkNotNull(indexedProperty, "Precondition violation - argument 'indexedProperty' must not be NULL!");
		checkNotNull(indexType, "Precondition violation - argument 'indexType' must not be NULL!");
		this.indexedProperty = indexedProperty;
		this.indexType = indexType;
	}

	@Override
	public String getId() {
		return this.getBackendIndexKey();
	}

	@Override
	public String getParentIndexId() {
		return null;
	}

	@Override
	public boolean isDirty() {
		return true;
	}

	@Override
	public String getBranch() {
		return ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
	}

	@Override
	public Period getValidPeriod() {
		return Period.eternal();
	}

	@Override
	public String getIndexedProperty() {
		return this.indexedProperty;
	}

	@Override
	public IndexType getIndexType() {
		return this.indexType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.indexType == null ? 0 : this.indexType.hashCode());
		result = prime * result + (this.indexedProperty == null ? 0 : this.indexedProperty.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		AbstractChronoGraphIndex2 other = (AbstractChronoGraphIndex2) obj;
		if (this.indexType != other.indexType) {
			return false;
		}
		if (this.indexedProperty == null) {
			if (other.indexedProperty != null) {
				return false;
			}
		} else if (!this.indexedProperty.equals(other.indexedProperty)) {
			return false;
		}
		return true;
	}

	@Override
	public Set<IndexingOption> getIndexingOptions() {
		return Collections.emptySet();
	}
}
