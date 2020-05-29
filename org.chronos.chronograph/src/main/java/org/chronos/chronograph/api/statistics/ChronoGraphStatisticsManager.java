package org.chronos.chronograph.api.statistics;

import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronodb.api.ChronoDBConstants;

public interface ChronoGraphStatisticsManager {

    /**
     * Returns the statistics for the "head portion" of the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
     *
     * <p>
     * The definition of what the "head portion" is, is up to the backend at hand. In general, it refers to the more recent history.
     *
     * @return The statistics. Never <code>null</code>.
     */
    public default BranchHeadStatistics getMasterBranchHeadStatistics(){
        return this.getBranchHeadStatistics(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    /**
     * Returns the statistics for the "head portion" of the given branch.
     *
     * <p>
     * The definition of what the "head portion" is, is up to the backend at hand. In general, it refers to the more recent history.
     *
     * @param branchName
     *            The name of the branch to retrieve the statistics for. Must not be <code>null</code>, must refer to an existing branch.
     *
     * @return The statistics. Never <code>null</code>.
     */
    public BranchHeadStatistics getBranchHeadStatistics(String branchName);

}
