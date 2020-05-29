package org.chronos.chronograph.internal.impl.iterators.builder.states;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.chronos.chronograph.api.iterators.states.AllChangedEdgeIdsAndTheirPreviousNeighborhoodState;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;

import java.util.Iterator;
import java.util.Set;

public class AllChangedEdgeIdsAndTheirPreviousNeighborhoodStateImpl extends GraphIteratorStateImpl implements AllChangedEdgeIdsAndTheirPreviousNeighborhoodState {

    private final String edgeId;
    private Set<String> neighborhoodVertexIds = null;

    public AllChangedEdgeIdsAndTheirPreviousNeighborhoodStateImpl(final ChronoGraph txGraph, String edgeId) {
        super(txGraph);
        this.edgeId = edgeId;

    }

    @Override
    public String getCurrentEdgeId() {
        return this.edgeId;
    }

    @Override
    public boolean isCurrentEdgeRemoved() {
        return !this.getEdgeById(this.edgeId).isPresent();
    }

    @Override
    public Set<String> getNeighborhoodVertexIds() {
        if (this.neighborhoodVertexIds == null) {
            this.neighborhoodVertexIds = this.calculatePreviousNeighborhoodIds();
        }
        return this.neighborhoodVertexIds;
    }

    private Set<String> calculatePreviousNeighborhoodIds() {
        Set<String> resultSet = Sets.newHashSet();
        // add the previous neighborhood (if any)
        String branch = this.getTransactionGraph().tx().getCurrentTransaction().getBranchName();
        Long previousCommit = this.getPreviousCommitOnEdge(this.getTransactionGraph(), this.edgeId);
        if (previousCommit != null) {
            try(ChronoGraph txGraph = this.getTransactionGraph().tx().createThreadedTx(branch, previousCommit)){
                Iterator<Edge> previousEdges = txGraph.edges(this.edgeId);
                if (previousEdges.hasNext()) {
                    Edge previousEdge = Iterators.getOnlyElement(previousEdges);
                    previousEdge.vertices(Direction.BOTH).forEachRemaining(v -> resultSet.add((String) v.id()));
                }
            }
        }
        return resultSet;
    }

    private Long getPreviousCommitOnEdge(ChronoGraph graph, String edgeId) {
        long txTimestamp = graph.tx().getCurrentTransaction().getTimestamp();
        Iterator<Long> historyIterator = graph.getEdgeHistory(edgeId);
        while (historyIterator.hasNext()) {
            long timestamp = historyIterator.next();
            if (timestamp < txTimestamp) {
                return timestamp;
            }
        }
        return null;
    }


}
