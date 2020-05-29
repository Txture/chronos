package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.api.transaction.trigger.CurrentState;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.NoTransactionControlChronoGraph;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class PrePersistCurrentStateImpl implements CurrentState {

    private final ChronoGraph originalGraph;
    private final ChronoGraph noTxControlGraph;

    public PrePersistCurrentStateImpl(ChronoGraph graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.originalGraph = graph;
        this.noTxControlGraph = new NoTransactionControlChronoGraph(graph);
    }

    @Override
    public long getTimestamp() {
        // we use the original graph here, rather than the wrapped one (the wrapped graph may prevent this call)
        return this.originalGraph.tx().getCurrentTransaction().getTimestamp();
    }

    @Override
    public GraphBranch getBranch() {
        // we use the original graph here, rather than the wrapped one (the wrapped graph may prevent this call)
        ChronoGraph g = this.originalGraph;
        return g.getBranchManager().getBranch(g.tx().getCurrentTransaction().getBranchName());
    }

    @Override
    public Set<ChronoVertex> getModifiedVertices() {
        ChronoGraphTransaction tx = this.originalGraph.tx().getCurrentTransaction();
        return Collections.unmodifiableSet(tx.getContext().getModifiedVertices());
    }

    @Override
    public Set<ChronoEdge> getModifiedEdges() {
        ChronoGraphTransaction tx = this.originalGraph.tx().getCurrentTransaction();
        return Collections.unmodifiableSet(tx.getContext().getModifiedEdges());
    }

    @Override
    public Set<String> getModifiedGraphVariables() {
        return this.getModifiedGraphVariables(ChronoGraphConstants.VARIABLES_DEFAULT_KEYSPACE);
    }

    @Override
    public Set<String> getModifiedGraphVariables(final String keyspace) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        ChronoGraphTransaction tx = this.originalGraph.tx().getCurrentTransaction();
        return Collections.unmodifiableSet(tx.getContext().getModifiedVariables(keyspace));
    }

    @Override
    public ChronoGraph getGraph() {
        return this.noTxControlGraph;
    }

}
