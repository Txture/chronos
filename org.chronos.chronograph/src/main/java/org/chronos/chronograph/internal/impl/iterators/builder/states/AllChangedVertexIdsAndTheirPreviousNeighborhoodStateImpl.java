package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.Set;

public class AllChangedVertexIdsAndTheirPreviousNeighborhoodStateImpl extends AllChangedVertexIdsStateImpl implements AllChangedVertexIdsAndTheirPreviousNeighborhoodState {


    private Set<String> neighborhoodEdgeIds = null;

    public AllChangedVertexIdsAndTheirPreviousNeighborhoodStateImpl(final ChronoGraph txGraph, String vertexId) {
        super(txGraph, vertexId);
    }


    @Override
    public Set<String> getNeighborhoodEdgeIds() {
        if (this.neighborhoodEdgeIds == null) {
            this.neighborhoodEdgeIds = this.calculatPreviousNeighborhoodEdgeIds(this.getCurrentVertexId());
        }
        return this.neighborhoodEdgeIds;
    }

}
