package org.chronos.chronograph.internal.impl.iterators.builder.states;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.chronos.chronograph.api.iterators.states.AllEdgesState;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public class AllEdgesStateImpl extends GraphIteratorStateImpl implements AllEdgesState {

    private final Edge edge;

    public AllEdgesStateImpl(final ChronoGraph txGraph, Edge edge) {
        super(txGraph);
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.edge = edge;
    }

    @Override
    public Edge getCurrentEdge() {
        return this.edge;
    }
}

