package org.chronos.chronodb.internal.impl.index.diff;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;

public class MutableIndexValueDiff implements IndexValueDiff {

	private final Object oldValue;
	private final Object newValue;

	private SetMultimap<SecondaryIndex, Object> indexToAdditions;
	private SetMultimap<SecondaryIndex, Object> indexToRemovals;

	public MutableIndexValueDiff(final Object oldValue, final Object newValue) {
		this.oldValue = oldValue;
		this.newValue = newValue;
	}

	@Override
	public Object getOldValue() {
		return this.oldValue;
	}

	@Override
	public Object getNewValue() {
		return this.newValue;
	}

	@Override
	public Set<Object> getAdditions(final SecondaryIndex index) {
		if (this.indexToAdditions == null || this.indexToAdditions.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(this.indexToAdditions.get(index));
	}

	@Override
	public Set<Object> getRemovals(final SecondaryIndex indexName) {
		if (this.indexToRemovals == null || this.indexToRemovals.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.unmodifiableSet(this.indexToRemovals.get(indexName));
	}

	@Override
	public Set<SecondaryIndex> getChangedIndices() {
		if (this.isEmpty()) {
			return Collections.emptySet();
		}
		if (this.indexToAdditions == null || this.indexToAdditions.isEmpty()) {
			return Collections.unmodifiableSet(this.indexToRemovals.keySet());
		}
		if (this.indexToRemovals == null || this.indexToRemovals.isEmpty()) {
			return Collections.unmodifiableSet(this.indexToAdditions.keySet());
		}
		Set<SecondaryIndex> changedIndices = Sets.union(this.indexToAdditions.keySet(), this.indexToRemovals.keySet());
		return Collections.unmodifiableSet(changedIndices);
	}

	@Override
	public boolean isEmpty() {
		// both change sets are either NULL (never touched) or empty (touched and then cleared)
		if ((this.indexToAdditions == null || this.indexToAdditions.isEmpty())
				&& (this.indexToRemovals == null || this.indexToRemovals.isEmpty())) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean isAdditive() {
		if (this.indexToAdditions != null && this.indexToAdditions.size() > 0) {
			if (this.indexToRemovals == null || this.indexToRemovals.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isSubtractive() {
		if (this.indexToRemovals != null && this.indexToRemovals.size() > 0) {
			if (this.indexToAdditions == null || this.indexToAdditions.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isMixed() {
		if (this.indexToAdditions != null && this.indexToAdditions.size() > 0) {
			if (this.indexToRemovals != null && this.indexToRemovals.size() > 0) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isIndexChanged(final SecondaryIndex index) {
		if (this.indexToAdditions != null && this.indexToAdditions.containsKey(index)) {
			return true;
		} else if (this.indexToRemovals != null && this.indexToRemovals.containsKey(index)) {
			return true;
		}
		return false;
	}

	public void add(final SecondaryIndex index, final Object value) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		if (this.isEntryRemoval()) {
			throw new IllegalStateException(
					"Cannot insert additive diff values to a diff that represents an entry removal!");
		}
		if (this.indexToAdditions == null) {
			this.indexToAdditions = HashMultimap.create();
		}
		this.indexToAdditions.put(index, value);
		if (this.indexToRemovals != null) {
			this.indexToRemovals.remove(index, value);
		}
	}

	public void removeSingleValue(final SecondaryIndex index, final Object value) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
		if (this.isEntryAddition()) {
			throw new IllegalStateException(
					"Cannot insert subtractive diff values to a diff that represents an entry addition!");
		}
		if (this.indexToRemovals == null) {
			this.indexToRemovals = HashMultimap.create();
		}
		this.indexToRemovals.put(index, value);
		if (this.indexToAdditions != null) {
			this.indexToAdditions.remove(index, value);
		}
	}

	public void removeMultipleValues(final SecondaryIndex index, final Set<Object> values) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		if (values == null || values.isEmpty()) {
			return;
		}
		for (Object value : values) {
			this.removeSingleValue(index, value);
		}
	}

}
