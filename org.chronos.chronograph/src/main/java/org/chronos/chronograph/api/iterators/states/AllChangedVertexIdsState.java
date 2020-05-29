package org.chronos.chronograph.api.iterators.states;

public interface AllChangedVertexIdsState extends GraphIteratorState {

    public String getCurrentVertexId();

    public boolean isCurrentVertexRemoved();

}
