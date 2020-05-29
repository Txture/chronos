package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.PreCommitStoreState;

import static com.google.common.base.Preconditions.*;

public class PreCommitStoreStateImpl implements PreCommitStoreState {

    private final ChronoGraph storeStateGraph;

    public PreCommitStoreStateImpl(ChronoGraph storeStateGraph){
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
