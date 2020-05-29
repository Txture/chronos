package org.chronos.chronograph.internal.impl.statistics;

import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronograph.api.statistics.ChronoGraphStatisticsManager;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphStatisticsManagerImpl implements ChronoGraphStatisticsManager {

    private final ChronoGraphInternal graph;

    public ChronoGraphStatisticsManagerImpl(ChronoGraphInternal graph){
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;

    }

    @Override
    public BranchHeadStatistics getBranchHeadStatistics(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        return this.graph.getBackingDB().getStatisticsManager().getBranchHeadStatistics(branchName);
    }
}
