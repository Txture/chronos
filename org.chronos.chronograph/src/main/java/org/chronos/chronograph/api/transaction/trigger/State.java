package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;

public interface State {

    public ChronoGraph getGraph();

    public long getTimestamp();

    public GraphBranch getBranch();
}
