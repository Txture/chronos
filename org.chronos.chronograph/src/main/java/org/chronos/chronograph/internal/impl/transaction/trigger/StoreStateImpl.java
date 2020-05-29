package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.StoreState;

import static com.google.common.base.Preconditions.*;

public class StoreStateImpl implements StoreState {

    private final ChronoGraph storeStateGraph;

    public StoreStateImpl(ChronoGraph storeStateGraph){
        checkNotNull(storeStateGraph, "Precondition violation - argument 'storeStateGraph' must not be NULL!");
        this.storeStateGraph = storeStateGraph;
    }

    @Override
    public long getTimestamp() {
        return this.storeStateGraph.tx().getCurrentTransaction().getTimestamp();
    }

    @Override
    public GraphBranch getBranch() {
        ChronoGraph g = this.storeStateGraph;
        return g.getBranchManager().getBranch(g.tx().getCurrentTransaction().getBranchName());
    }

    @Override
    public ChronoGraph getGraph() {
        return this.storeStateGraph;
    }

}
