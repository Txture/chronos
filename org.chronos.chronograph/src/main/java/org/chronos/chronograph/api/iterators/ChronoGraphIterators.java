package org.chronos.chronograph.api.iterators;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.iterators.builder.ChronoGraphRootIteratorBuilderImpl;

import static com.google.common.base.Preconditions.*;

public interface ChronoGraphIterators {

    public static ChronoGraphRootIteratorBuilder createIteratorOn(ChronoGraph graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkArgument(graph.isClosed() == false, "Precondition violation - argument 'graph' must not be closed!");
        return new ChronoGraphRootIteratorBuilderImpl(graph);
    }

}
