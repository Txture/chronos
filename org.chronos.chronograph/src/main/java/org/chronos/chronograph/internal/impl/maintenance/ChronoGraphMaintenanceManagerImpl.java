package org.chronos.chronograph.internal.impl.maintenance;

import org.chronos.chronograph.api.maintenance.ChronoGraphMaintenanceManager;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphMaintenanceManagerImpl implements ChronoGraphMaintenanceManager {

    private final ChronoGraphInternal graph;

    public ChronoGraphMaintenanceManagerImpl(ChronoGraphInternal graph){
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }


    @Override
    public void performRolloverOnBranch(final String branchName, boolean updateIndices) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.graph.getBackingDB().getMaintenanceManager().performRolloverOnBranch(branchName, updateIndices);
    }

    @Override
    public void performRolloverOnAllBranches(boolean updateIndices) {
        this.graph.getBackingDB().getMaintenanceManager().performRolloverOnAllBranches(updateIndices);
    }

    @Override
    public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate, boolean updateIndices) {
        checkNotNull(branchPredicate, "Precondition violation - argument 'branchPredicate' must not be NULL!");
        this.graph.getBackingDB().getMaintenanceManager().performRolloverOnAllBranchesWhere(branchPredicate, updateIndices);
    }
}
