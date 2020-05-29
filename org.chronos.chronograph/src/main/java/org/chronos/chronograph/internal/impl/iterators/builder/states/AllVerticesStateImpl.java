package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.iterators.states.AllVerticesState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllVerticesStateImpl extends GraphIteratorStateImpl implements AllVerticesState {

    private final Vertex vertex;

    public AllVerticesStateImpl(final ChronoGraph txGraph, Vertex vertex) {
        super(txGraph);
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        this.vertex = vertex;
    }

    @Override
    public Vertex getCurrentVertex() {
        return this.vertex;
    }
}
