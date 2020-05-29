package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllChangedVertexIdsStateImpl extends GraphIteratorStateImpl implements AllChangedVertexIdsState {

    private final String vertexId;

    public AllChangedVertexIdsStateImpl(final ChronoGraph txGraph, String vertexId) {
        super(txGraph);
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        this.vertexId = vertexId;
    }

    @Override
    public String getCurrentVertexId() {
        return this.vertexId;
    }

    @Override
    public boolean isCurrentVertexRemoved() {
        return !this.getVertexById(this.vertexId).isPresent();
    }
}
