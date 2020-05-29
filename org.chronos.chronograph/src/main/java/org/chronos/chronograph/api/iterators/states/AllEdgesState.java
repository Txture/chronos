package org.chronos.chronograph.api.iterators.states;

import org.apache.tinkerpop.gremlin.structure.Edge;

public interface AllEdgesState extends GraphIteratorState {

    public Edge getCurrentEdge();

}
