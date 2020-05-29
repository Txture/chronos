package org.chronos.chronodb.api;

import org.chronos.chronodb.internal.impl.BranchHeadStatisticsImpl;

import static com.google.common.base.Preconditions.*;

public interface BranchHeadStatistics {

    public long getTotalNumberOfEntries();

    public long getNumberOfEntriesInHead();

    public long getNumberOfEntriesInHistory();

    public double getHeadHistoryRatio();

}
