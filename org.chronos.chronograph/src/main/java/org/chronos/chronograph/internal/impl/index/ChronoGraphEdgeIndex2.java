package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.IChronoGraphEdgeIndex;
import org.chronos.common.exceptions.UnknownEnumLiteralException;


/**
 * @deprecated  Use {@link ChronoGraphIndex3} instead. This class only exists for backwards compatibility.
 */
@Deprecated
public class ChronoGraphEdgeIndex2 extends AbstractChronoGraphIndex2 implements IChronoGraphEdgeIndex {

	protected ChronoGraphEdgeIndex2() {
		// default constructor for serialization
	}

	public ChronoGraphEdgeIndex2(final String indexedProperty, final IndexType indexType) {
		super(indexedProperty, indexType);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String getBackendIndexKey() {
		return ChronoGraphConstants.INDEX_PREFIX_EDGE + this.getIndexedProperty();
	}

	@Override
	public Class<? extends Element> getIndexedElementClass() {
		return Edge.class;
	}

	@Override
	public String toString() {
		return "Index[Edge, " + this.getIndexedProperty() + ", " + this.indexType + "]";
	}

}
