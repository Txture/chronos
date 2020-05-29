package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.iterators.states.AllChangedElementIdsAndTheirNeighborhoodState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.Collections;
import java.util.Set;

public class AllChangedElementIdsAndTheirPreviousNeighborhoodStateImpl extends AllChangedElementIdsStateImpl implements AllChangedElementIdsAndTheirNeighborhoodState {

    private Set<String> neighborhoodEdgeIds = null;
    private Set<String> neighborhoodVertexIds = null;

    public AllChangedElementIdsAndTheirPreviousNeighborhoodStateImpl(final ChronoGraph txGraph, String elementId, Class<? extends Element> elementType) {
        super(txGraph, elementId, elementType);
        if (this.isCurrentElementAVertex()) {
            // a vertex can't have vertices in its direct neighborhood
            this.neighborhoodVertexIds = Collections.emptySet();
        } else {
            // and edge can't have edges in its direct neighborhood
            this.neighborhoodEdgeIds = Collections.emptySet();
        }
    }

    @Override
    public Set<String> getNeighborhoodVertexIds() {
        return null;
    }

    @Override
    public Set<String> getNeighborhoodEdgeIds() {
        if (this.neighborhoodEdgeIds == null) {
            this.neighborhoodEdgeIds = this.calculatPreviousNeighborhoodEdgeIds(this.getCurrentElementId());
        }
        return this.neighborhoodEdgeIds;
    }

    @Override
    public boolean isCurrentElementAVertex() {
        return Vertex.class.isAssignableFrom(this.getCurrentElementClass());
    }

    @Override
    public boolean isCurrentElementAnEdge() {
        return Edge.class.isAssignableFrom(this.getCurrentElementClass());
    }
}
