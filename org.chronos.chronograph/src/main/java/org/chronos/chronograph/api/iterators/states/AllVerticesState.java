package org.chronos.chronograph.api.iterators.states;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface AllVerticesState extends GraphIteratorState {

    public Vertex getCurrentVertex();


}
