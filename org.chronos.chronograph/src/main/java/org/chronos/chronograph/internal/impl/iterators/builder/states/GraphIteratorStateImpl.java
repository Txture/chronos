package org.chronos.chronograph.internal.impl.iterators.builder.states;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.iterators.states.GraphIteratorState;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class GraphIteratorStateImpl implements GraphIteratorState {

    private final ChronoGraph txGraph;

    public GraphIteratorStateImpl(ChronoGraph txGraph) {
        checkNotNull(txGraph, "Precondition violation - argument 'txGraph' must not be NULL!");
        checkArgument(txGraph.tx().isOpen(), "Precondition violation - argument 'txGraph' must have an open transaction!");
        this.txGraph = txGraph;
    }


    @Override
    public ChronoGraph getTransactionGraph() {
        return this.txGraph;
    }


    protected Set<String> calculatPreviousNeighborhoodEdgeIds(String vertexId) {
        Set<String> resultSet = Sets.newHashSet();
        // add the previous neighborhood (if any)
        String branch = this.getTransactionGraph().tx().getCurrentTransaction().getBranchName();
        Long previousCommit = this.getPreviousCommitOnVertex(this.getTransactionGraph(), vertexId);
        if (previousCommit != null) {
            try(ChronoGraph previousTxGraph = this.getTransactionGraph().tx().createThreadedTx(branch, previousCommit)) {
                Iterator<Vertex> previousVertices = previousTxGraph.vertices(vertexId);
                if (previousVertices.hasNext()) {
                    Vertex previousVertex = Iterators.getOnlyElement(previousVertices);
                    previousVertex.edges(Direction.BOTH).forEachRemaining(e -> resultSet.add((String) e.id()));
                }
            }
        }
        return resultSet;
    }

    protected Long getPreviousCommitOnVertex(ChronoGraph graph, String vertexId) {
        long txTimestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<Long> historyIterator = graph.getVertexHistory(vertexId);
        while (historyIterator.hasNext()) {
            long timestamp = historyIterator.next();
            if (timestamp < txTimestamp) {
                return timestamp;
            }
        }
        return null;
    }
}
