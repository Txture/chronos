package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.AncestorState;

import static com.google.common.base.Preconditions.*;

public class AncestorStateImpl implements AncestorState {

    private final ChronoGraph ancestorGraph;

    public AncestorStateImpl(ChronoGraph ancestorGraph){
        checkNotNull(ancestorGraph, "Precondition violation - argument 'ancestorGraph' must not be NULL!");
        this.ancestorGraph = ancestorGraph;
    }

    @Override
    public long getTimestamp() {
        return this.ancestorGraph.tx().getCurrentTransaction().getTimestamp();
    }

    @Override
    public GraphBranch getBranch() {
        ChronoGraph g = this.ancestorGraph;
        return g.getBranchManager().getBranch(g.tx().getCurrentTransaction().getBranchName());
    }


    @Override
    public ChronoGraph getGraph() {
        return this.ancestorGraph;
    }

}
