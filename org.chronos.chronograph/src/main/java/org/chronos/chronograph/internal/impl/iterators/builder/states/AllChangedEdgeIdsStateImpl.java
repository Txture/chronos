package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllChangedEdgeIdsStateImpl extends GraphIteratorStateImpl implements AllChangedEdgeIdsState {

    private final String edgeId;

    public AllChangedEdgeIdsStateImpl(final ChronoGraph txGraph, String edgeId) {
        super(txGraph);
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        this.edgeId = edgeId;
    }


    @Override
    public String getCurrentEdgeId() {
        return this.edgeId;
    }

    @Override
    public boolean isCurrentEdgeRemoved() {
        return !this.getEdgeById(this.edgeId).isPresent();
    }
}
