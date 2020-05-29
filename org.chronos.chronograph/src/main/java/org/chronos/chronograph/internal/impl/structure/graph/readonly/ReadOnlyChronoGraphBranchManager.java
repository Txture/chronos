package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.branch.ChronoGraphBranchManager;
import org.chronos.chronograph.api.branch.GraphBranch;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphBranchManager implements ChronoGraphBranchManager {

    private final ChronoGraphBranchManager manager;

    public ReadOnlyChronoGraphBranchManager(ChronoGraphBranchManager manager){
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }

    @Override
    public GraphBranch createBranch(final String branchName) {
        return this.unsupportedOperation();
    }

    @Override
    public GraphBranch createBranch(final String branchName, final long branchingTimestamp) {
        return this.unsupportedOperation();
    }

    @Override
    public GraphBranch createBranch(final String parentName, final String newBranchName) {
        return this.unsupportedOperation();
    }

    @Override
    public GraphBranch createBranch(final String parentName, final String newBranchName, final long branchingTimestamp) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean existsBranch(final String branchName) {
        return this.manager.existsBranch(branchName);
    }

    @Override
    public GraphBranch getBranch(final String branchName) {
        return this.manager.getBranch(branchName);
    }

    @Override
    public Set<String> getBranchNames() {
        return Collections.unmodifiableSet(this.manager.getBranchNames());
    }

    @Override
    public Set<GraphBranch> getBranches() {
        return Collections.unmodifiableSet(this.manager.getBranches());
    }

    @Override
    public List<String> deleteBranchRecursively(final String branchName) {
        return this.unsupportedOperation();
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported on a readOnly graph!");
    }
}
