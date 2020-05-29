package org.chronos.chronograph.api.iterators.states;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Preconditions.*;

public interface GraphIteratorState {

    public ChronoGraph getTransactionGraph();

    public default String getBranch() {
        return this.getTransactionGraph().tx().getCurrentTransaction().getBranchName();
    }

    public default long getTimestamp() {
        return this.getTransactionGraph().tx().getCurrentTransaction().getTimestamp();
    }

    public default Optional<Vertex> getVertexById(String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        Iterator<Vertex> vertices = this.getTransactionGraph().vertices(vertexId);
        if (vertices.hasNext() == false) {
            return Optional.empty();
        } else {
            return Optional.of(Iterators.getOnlyElement(vertices));
        }
    }

    public default Optional<Edge> getEdgeById(String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        Iterator<Edge> edges = this.getTransactionGraph().edges(edgeId);
        if (edges.hasNext() == false) {
            return Optional.empty();
        } else {
            return Optional.of(Iterators.getOnlyElement(edges));
        }
    }

}
