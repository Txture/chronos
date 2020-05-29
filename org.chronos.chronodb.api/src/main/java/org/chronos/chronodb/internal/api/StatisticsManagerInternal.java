package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.StatisticsManager;

public interface StatisticsManagerInternal extends StatisticsManager {

    public void updateBranchHeadStatistics(String branchName, long inserts, long updates, long deletes);

    public void clearBranchHeadStatistics();

    public void clearBranchHeadStatistics(String branchName);
}
