package org.chronos.chronograph.api.iterators.states;

import java.util.Set;

public interface AllChangedEdgeIdsAndTheirPreviousNeighborhoodState extends GraphIteratorState {

    public String getCurrentEdgeId();

    public boolean isCurrentEdgeRemoved();

    public Set<String> getNeighborhoodVertexIds();

}
