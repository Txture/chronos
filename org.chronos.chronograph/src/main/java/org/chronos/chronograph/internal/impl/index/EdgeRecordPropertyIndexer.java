package org.chronos.chronograph.internal.impl.index;

import java.util.Optional;
import java.util.Set;

import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.common.annotation.PersistentClass;

/**
 * An indexer working on {@link EdgeRecord}s.
 *
 * @deprecated Superseded by {@link EdgeRecordPropertyIndexer2}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
@Deprecated
@PersistentClass("kryo")
public class EdgeRecordPropertyIndexer extends AbstractRecordPropertyIndexer implements StringIndexer {

	protected EdgeRecordPropertyIndexer() {
		// default constructor for serializer
	}

	public EdgeRecordPropertyIndexer(final String propertyName) {
		super(propertyName);
	}

	@Override
	public boolean canIndex(final Object object) {
		return object instanceof IEdgeRecord;
	}

	@Override
	public Set<String> getIndexValues(final Object object) {
		IEdgeRecord edgeRecord = (IEdgeRecord) object;
		Optional<? extends IPropertyRecord> maybePropertyRecord = edgeRecord.getProperties().stream()
				.filter(pRecord -> pRecord.getKey().equals(this.propertyName)).findAny();
		return this.getIndexValue(maybePropertyRecord);
	}

}
