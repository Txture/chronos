package org.chronos.chronograph.api.iterators.states;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface AllChangedElementIdsState extends GraphIteratorState {

    public String getCurrentElementId();

    public Class<? extends Element> getCurrentElementClass();

    public boolean isCurrentElementRemoved();

    public default boolean isCurrentElementAVertex() {
        return Vertex.class.isAssignableFrom(this.getCurrentElementClass());
    }

    public default boolean isCurrentElementAnEdge() {
        return Edge.class.isAssignableFrom(this.getCurrentElementClass());
    }

}
