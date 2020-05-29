package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.MaintenanceManager;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class InMemoryMaintenanceManager implements MaintenanceManager {

    @SuppressWarnings("unused")
    private final InMemoryChronoDB owningDB;

    public InMemoryMaintenanceManager(final InMemoryChronoDB owningDB) {
        checkNotNull(owningDB, "Precondition violation - argument 'owningDB' must not be NULL!");
        this.owningDB = owningDB;
    }

    // =================================================================================================================
    // ROLLOVER
    // =================================================================================================================

    @Override
    public void performRolloverOnBranch(final String branchName, boolean updateIndices) {
        throw new UnsupportedOperationException("The in-memory backend does not support rollover operations.");
    }

    @Override
    public void performRolloverOnAllBranches(boolean updateIndices) {
        throw new UnsupportedOperationException("The in-memory backend does not support rollover operations.");
    }

    @Override
    public void performRolloverOnAllBranchesWhere(final Predicate<String> branchPredicate, boolean updateIndices) {
        throw new UnsupportedOperationException("The in-memory backend does not support rollover operations.");
    }

}
