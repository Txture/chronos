package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.IChronoGraphVertexIndex;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

/**
 * @deprecated  Use {@link ChronoGraphIndex3} instead. This class only exists for backwards compatibility.
 */
@Deprecated
public class ChronoGraphVertexIndex2 extends AbstractChronoGraphIndex2 implements IChronoGraphVertexIndex {

	protected ChronoGraphVertexIndex2() {
		// default constructor for serialization
	}

	public ChronoGraphVertexIndex2(final String indexedProperty, final IndexType indexType) {
		super(indexedProperty, indexType);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public String getBackendIndexKey() {
		return ChronoGraphConstants.INDEX_PREFIX_VERTEX + this.getIndexedProperty();
	}

	@Override
	public Class<? extends Element> getIndexedElementClass() {
		return Vertex.class;
	}

	@Override
	public String toString() {
		return "Index[Vertex, " + this.getIndexedProperty() + ", " + this.indexType + "]";
	}

}
