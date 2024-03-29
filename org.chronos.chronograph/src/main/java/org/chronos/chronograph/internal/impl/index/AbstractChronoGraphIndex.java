package org.chronos.chronograph.internal.impl.index;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Set;

/**
 * This class represents the definition of a graph index (index metadata) on disk.
 *
 * @deprecated Superseded by {@link AbstractChronoGraphIndex2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public abstract class AbstractChronoGraphIndex implements ChronoGraphIndexInternal {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String indexedProperty;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	protected AbstractChronoGraphIndex() {
		// default constructor for serialization
	}

	public AbstractChronoGraphIndex(final String indexedProperty) {
		checkNotNull(indexedProperty, "Precondition violation - argument 'indexedProperty' must not be NULL!");
		this.indexedProperty = indexedProperty;
	}

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
		// the old version represented by this class only supported strings
		return IndexType.STRING;
	}

	@Override
	public Set<IndexingOption> getIndexingOptions() {
		return Collections.emptySet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		AbstractChronoGraphIndex other = (AbstractChronoGraphIndex) obj;
		if (this.indexedProperty == null) {
			if (other.indexedProperty != null) {
				return false;
			}
		} else if (!this.indexedProperty.equals(other.indexedProperty)) {
			return false;
		}
		return true;
	}

}
