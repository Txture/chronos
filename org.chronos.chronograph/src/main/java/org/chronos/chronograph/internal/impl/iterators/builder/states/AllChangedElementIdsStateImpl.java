package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllChangedElementIdsStateImpl extends GraphIteratorStateImpl implements AllChangedElementIdsState {

    private final String elementId;
    private final Class<? extends Element> elementType;

    public AllChangedElementIdsStateImpl(final ChronoGraph txGraph, String elementId, Class<? extends Element> elementType) {
        super(txGraph);
        checkNotNull(elementId, "Precondition violation - argument 'elementId' must not be NULL!");
        checkNotNull(elementType, "Precondition violation - argument 'elementType' must not be NULL!");
        if (!Vertex.class.isAssignableFrom(elementType) && !Edge.class.isAssignableFrom(elementType)) {
            throw new IllegalArgumentException("Precondition violation - argument 'elementType' is neither Vertex nor Edge!");
        }
        this.elementId = elementId;
        this.elementType = elementType;
    }

    @Override
    public String getCurrentElementId() {
        return this.elementId;
    }

    @Override
    public Class<? extends Element> getCurrentElementClass() {
        return this.elementType;
    }

    @Override
    public boolean isCurrentElementRemoved() {
        if (this.isCurrentElementAVertex()) {
            return !this.getVertexById(this.elementId).isPresent();
        } else {
            return !this.getEdgeById(this.elementId).isPresent();
        }
    }
}
