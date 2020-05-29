package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronograph.api.statistics.ChronoGraphStatisticsManager;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphStatisticsManager implements ChronoGraphStatisticsManager {

    private final ChronoGraphStatisticsManager manager;

    public ReadOnlyChronoGraphStatisticsManager(final ChronoGraphStatisticsManager statisticsManager) {
        checkNotNull(statisticsManager, "Precondition violation - argument 'statisticsManager' must not be NULL!");
        this.manager = statisticsManager;
    }


    @Override
    public BranchHeadStatistics getBranchHeadStatistics(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        return this.manager.getBranchHeadStatistics(branchName);
    }
}
