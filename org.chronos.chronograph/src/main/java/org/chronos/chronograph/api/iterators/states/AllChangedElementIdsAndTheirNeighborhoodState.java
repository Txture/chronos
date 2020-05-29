package org.chronos.chronograph.api.iterators.states;

import java.util.Set;

public interface AllChangedElementIdsAndTheirNeighborhoodState extends AllChangedElementIdsState {

    public Set<String> getNeighborhoodVertexIds();

    public Set<String> getNeighborhoodEdgeIds();


}
