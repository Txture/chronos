package org.chronos.chronograph.api.iterators.states;

import java.util.Set;

public interface AllChangedVertexIdsAndTheirPreviousNeighborhoodState extends GraphIteratorState {

    public String getCurrentVertexId();

    public boolean isCurrentVertexRemoved();

    public Set<String> getNeighborhoodEdgeIds();

}
