package org.chronos.chronograph.api.iterators.states;

public interface AllChangedEdgeIdsState extends GraphIteratorState {

    public String getCurrentEdgeId();

    public boolean isCurrentEdgeRemoved();

}
