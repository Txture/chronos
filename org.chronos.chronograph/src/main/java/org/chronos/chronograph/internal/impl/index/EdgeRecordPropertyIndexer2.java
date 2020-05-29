package org.chronos.chronograph.internal.impl.index;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.common.annotation.PersistentClass;

/**
 * An indexer working on {@link EdgeRecord}s.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 * @param <T>
 *            The type of values produced by this indexer.
 */
@PersistentClass("kryo")
public abstract class EdgeRecordPropertyIndexer2<T> extends AbstractRecordPropertyIndexer2<T> {

	protected EdgeRecordPropertyIndexer2() {
		// default constructor for serializer
	}

	public EdgeRecordPropertyIndexer2(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof IEdgeRecord;
	}

	@Override
	public Set<T> getIndexValues(final Object object) {
		IEdgeRecord vertexRecord = (IEdgeRecord) object;
		Optional<? extends IPropertyRecord> maybePropertyRecord = vertexRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return maybePropertyRecord.map(this::getIndexValuesInternal).orElse(Collections.emptySet());
	}

	protected abstract Set<T> getIndexValuesInternal(IPropertyRecord record);

}
