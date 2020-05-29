package org.chronos.chronograph.api.iterators;

import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsState;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsAndTheirNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsState;
import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.iterators.states.AllChangedVertexIdsState;
import org.chronos.chronograph.api.iterators.states.AllEdgesState;
import org.chronos.chronograph.api.iterators.states.AllElementsState;
import org.chronos.chronograph.api.iterators.states.AllVerticesState;
import org.chronos.chronograph.api.iterators.states.GraphIteratorState;

import java.util.function.Consumer;

public interface ChronoGraphTimestampIteratorBuilder extends ChronoGraphIteratorBuilder {

    public void visitCoordinates(Consumer<GraphIteratorState> consumer);

    public void overAllVertices(Consumer<AllVerticesState> consumer);

    public void overAllEdges(Consumer<AllEdgesState> consumer);

    public void overAllElements(Consumer<AllElementsState> consumer);

    public void overAllChangedVertexIds(Consumer<AllChangedVertexIdsState> consumer);

    public void overAllChangedEdgeIds(Consumer<AllChangedEdgeIdsState> consumer);

    public void overAllChangedElementIds(Consumer<AllChangedElementIdsState> consumer);

    public void overAllChangedVertexIdsAndTheirPreviousNeighborhood(Consumer<AllChangedVertexIdsAndTheirPreviousNeighborhoodState> consumer);

    public void overAllChangedEdgeIdsAndTheirPreviousNeighborhood(Consumer<AllChangedEdgeIdsAndTheirPreviousNeighborhoodState> consumer);

    public void overAllChangedElementIdsAndTheirPreviousNeighborhood(Consumer<AllChangedElementIdsAndTheirNeighborhoodState> consumer);


}
