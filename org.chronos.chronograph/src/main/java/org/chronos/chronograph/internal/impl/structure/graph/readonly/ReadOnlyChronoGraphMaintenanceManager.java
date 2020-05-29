package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.maintenance.ChronoGraphMaintenanceManager;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphMaintenanceManager implements ChronoGraphMaintenanceManager {

    private final ChronoGraphMaintenanceManager manager;

    public ReadOnlyChronoGraphMaintenanceManager(ChronoGraphMaintenanceManager manager){
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }

    @Override
    public void performRolloverOnBranch(final String branchName, boolean updateIndices) {
        this.unsupportedOperation();
    }

    @Override
    public void performRolloverOnAllBranches(boolean updateIndices) {
        this.unsupportedOperation();
    }

    @Override
    public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate, boolean updateIndices) {
        this.unsupportedOperation();
    }

    private <T> T unsupportedOperation(){
        throw new UnsupportedOperationException("This operation is not supported on a read-only graph!");
    }
}
