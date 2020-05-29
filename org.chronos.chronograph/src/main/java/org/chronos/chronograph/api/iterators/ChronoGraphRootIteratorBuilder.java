package org.chronos.chronograph.api.iterators;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.iterators.callbacks.BranchChangeCallback;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public interface ChronoGraphRootIteratorBuilder extends ChronoGraphIteratorBuilder {

    public default ChronoGraphBranchIteratorBuilder overAllBranches() {
        return this.overAllBranches(BranchChangeCallback.IGNORE);
    }

    public default ChronoGraphBranchIteratorBuilder overAllBranches(BranchChangeCallback callback) {
        return this.overAllBranches(GraphBranchIterationOrder.ORIGIN_FIRST, callback);
    }

    public default ChronoGraphBranchIteratorBuilder overAllBranches(Comparator<GraphBranch> comparator, BranchChangeCallback callback) {
        checkNotNull(comparator, "Precondition violation - argument 'comparator' must not be NULL!");
        checkNotNull(callback, "Precondition violation - argument 'callback' must not be NULL!");
        List<String> branchNames = this.getGraph().getBranchManager().getBranches().stream()
            .sorted(comparator)
            .map(GraphBranch::getName)
            .collect(Collectors.toList());
        return this.overBranches(branchNames, callback);
    }


    public default ChronoGraphBranchIteratorBuilder onMasterBranch() {
        return this.onBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    public default ChronoGraphBranchIteratorBuilder onBranch(String branchName) {
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        return this.overBranches(Collections.singleton(branchName));
    }

    public default ChronoGraphBranchIteratorBuilder overBranches(Iterable<String> branchNames) {
        checkNotNull(branchNames, "Precondition violation - argument 'branchNames' must not be NULL!");
        return this.overBranches(branchNames, BranchChangeCallback.IGNORE);
    }

    public ChronoGraphBranchIteratorBuilder overBranches(Iterable<String> branchNames, BranchChangeCallback callback);

}
