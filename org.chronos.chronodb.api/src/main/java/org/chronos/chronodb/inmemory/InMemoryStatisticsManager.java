package org.chronos.chronodb.inmemory;

import com.google.common.collect.Maps;
import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.engines.base.AbstractStatisticsManager;

import java.util.Map;

import static com.google.common.base.Preconditions.*;

public class InMemoryStatisticsManager extends AbstractStatisticsManager {

    private final ChronoDBInternal owningDB;
    private final Map<String, BranchHeadStatistics> branchHeadStatistics = Maps.newHashMap();

    public InMemoryStatisticsManager(ChronoDBInternal owningDB){
        checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
        this.owningDB = owningDB;
    }

    @Override
    protected BranchHeadStatistics loadBranchHeadStatistics(final String branch) {
        BranchHeadStatistics statistics = this.branchHeadStatistics.get(branch);
        if(statistics == null){
            BranchInternal b = this.owningDB.getBranchManager().getBranch(branch);
            if(b == null){
                throw new IllegalArgumentException("There is no branch named '" + branch + "'!");
            }
            statistics = b.getTemporalKeyValueStore().calculateBranchHeadStatistics();
            this.branchHeadStatistics.put(branch, statistics);
        }
        return statistics;
    }

    @Override
    protected void saveBranchHeadStatistics(final String branchName, final BranchHeadStatistics statistics) {
        this.branchHeadStatistics.put(branchName, statistics);
    }

    @Override
    public void deleteBranchHeadStatistics() {
        this.branchHeadStatistics.clear();
    }

    @Override
    public void deleteBranchHeadStatistics(final String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.branchHeadStatistics.remove(branchName);
    }
}
