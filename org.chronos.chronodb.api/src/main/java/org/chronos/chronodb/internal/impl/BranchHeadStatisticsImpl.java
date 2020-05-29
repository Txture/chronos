package org.chronos.chronodb.internal.impl;

import org.chronos.chronodb.api.BranchHeadStatistics;

import static com.google.common.base.Preconditions.*;

public class BranchHeadStatisticsImpl implements BranchHeadStatistics {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long entriesInHead;
    private long totalEntries;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public BranchHeadStatisticsImpl(long entriesInHead, long totalEntries) {
        checkArgument(entriesInHead >= 0, "Precondition violation, argument 'entriesInHead' must not be negative!");
        checkArgument(totalEntries >= 0, "Precondition violation, argument 'totalEntries' must not be negative!");
        this.entriesInHead = entriesInHead;
        this.totalEntries = totalEntries;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public long getTotalNumberOfEntries() {
        return this.totalEntries;
    }

    @Override
    public long getNumberOfEntriesInHead() {
        return this.entriesInHead;
    }

    @Override
    public long getNumberOfEntriesInHistory() {
        return this.totalEntries - this.entriesInHead;
    }

    @Override
    public double getHeadHistoryRatio() {
        if (this.totalEntries <= 0) {
            if(this.entriesInHead > 0){
                return this.entriesInHead;
            }else{
                return 1;
            }
        }
        return (double) this.entriesInHead / this.totalEntries;
    }
}
