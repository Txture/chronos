package org.chronos.chronograph.internal.impl.index;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.structure.record.Record;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

/**
 * A base class for all indexers working on {@link Record}s.
 *
 * @deprecated Superseded by {@link AbstractRecordPropertyIndexer2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public abstract class AbstractRecordPropertyIndexer implements StringIndexer {

	protected String propertyName;

	protected AbstractRecordPropertyIndexer() {
		// default constructor for serialization
	}

	protected AbstractRecordPropertyIndexer(final String propertyName) {
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		this.propertyName = propertyName;
	}

	protected Set<String> getIndexValue(final Optional<? extends IPropertyRecord> maybePropertyRecord) {
		if (maybePropertyRecord.isPresent() == false) {
			// the vertex doesn't have the property; nothing to index
			return Collections.emptySet();
		} else {
			IPropertyRecord property = maybePropertyRecord.get();
			Object value = property.getValue();
			if (value == null) {
				// this should actually never happen, just a safety measure
				return Collections.emptySet();
			}
			if (value instanceof Iterable) {
				// multiplicity-many property
				Iterable<?> iterable = (Iterable<?>) value;
				Set<String> indexedValues = Sets.newHashSet();
				for (Object element : iterable) {
					indexedValues.add(this.convertToString(element));
				}
				return Collections.unmodifiableSet(indexedValues);
			} else {
				// multiplicity-one property
				return Collections.singleton(this.convertToString(value));
			}
		}
	}

	protected String convertToString(final Object element) {
		return String.valueOf(element);
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		AbstractRecordPropertyIndexer that = (AbstractRecordPropertyIndexer) o;

		return propertyName != null ? propertyName.equals(that.propertyName) : that.propertyName == null;
	}

	@Override
	public int hashCode() {
		return propertyName != null ? propertyName.hashCode() : 0;
	}
}
