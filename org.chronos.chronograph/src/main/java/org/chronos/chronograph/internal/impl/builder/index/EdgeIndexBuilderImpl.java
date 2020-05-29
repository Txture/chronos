package org.chronos.chronograph.internal.impl.builder.index;

import org.chronos.chronograph.api.builder.index.EdgeIndexBuilder;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.ChronoGraphEdgeIndex2;
import org.chronos.chronograph.internal.impl.index.IndexType;

public class EdgeIndexBuilderImpl extends AbstractGraphElementIndexBuilder<EdgeIndexBuilder>
		implements EdgeIndexBuilder {

	protected EdgeIndexBuilderImpl(final ChronoGraphIndexManagerInternal manager, final String propertyName, final IndexType indexType) {
		super(manager, propertyName, indexType);
	}

	@Override
	public ChronoGraphIndex build() {
		return this.build(null);
	}

	@Override
	public ChronoGraphIndex build(Object commitMetadata) {
		ChronoGraphEdgeIndex2 index = new ChronoGraphEdgeIndex2(this.propertyName, this.indexType);
		this.manager.addIndex(index, commitMetadata);
		return index;
	}

}
