package org.chronos.chronograph.internal.impl.index;

import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.api.structure.record.Record;
import org.chronos.common.annotation.PersistentClass;

import java.util.Objects;

import static com.google.common.base.Preconditions.*;

/**
 * A base class for all indexers working on {@link Record}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The type of values produced by this indexer.
 */
@PersistentClass("kryo")
public abstract class AbstractRecordPropertyIndexer2<T> implements Indexer<T> {

	protected String propertyName;

	protected AbstractRecordPropertyIndexer2() {
		// default constructor for serialization
	}

	protected AbstractRecordPropertyIndexer2(final String propertyName) {
		checkNotNull(propertyName, "Precondition violation - argument 'propertyName' must not be NULL!");
		this.propertyName = propertyName;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		AbstractRecordPropertyIndexer2<?> that = (AbstractRecordPropertyIndexer2<?>) o;

		return Objects.equals(propertyName, that.propertyName);
	}

	@Override
	public int hashCode() {
		return propertyName != null ? propertyName.hashCode() : 0;
	}
}
