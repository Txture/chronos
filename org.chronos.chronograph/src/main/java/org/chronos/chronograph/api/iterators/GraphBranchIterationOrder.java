package org.chronos.chronograph.api.iterators;

import org.chronos.chronograph.api.branch.GraphBranch;

import java.util.Comparator;

import static com.google.common.base.Preconditions.*;

public enum GraphBranchIterationOrder implements Comparator<GraphBranch> {

    // =================================================================================================================
    // ENUM LITERALS
    // =================================================================================================================

    ORIGIN_FIRST(GraphBranchIterationOrder::compareOriginFirst),

    BRANCHING_TIMESTAMPS(GraphBranchIterationOrder::compareBranchingTimestamps);


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Comparator<GraphBranch> compareFunction;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    private GraphBranchIterationOrder(Comparator<GraphBranch> compareFunction) {
        checkNotNull(compareFunction, "Precondition violation - argument 'compareFunction' must not be NULL!");
        this.compareFunction = compareFunction;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public int compare(final GraphBranch o1, final GraphBranch o2) {
        return this.compareFunction.compare(o1, o2);
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private static int compareOriginFirst(final GraphBranch gb1, final GraphBranch gb2) {
        if (gb1 == null && gb2 == null) {
            return 0;
        }
        if (gb1 == null && gb2 != null) {
            return -1;
        }
        if (gb1 != null && gb2 == null) {
            return 1;
        }
        if (gb1.getOriginsRecursive().contains(gb2)) {
            return 1;
        } else if (gb2.getOriginsRecursive().contains(gb1)) {
            return -1;
        } else {
            return 0;
        }
    }

    private static int compareBranchingTimestamps(final GraphBranch gb1, final GraphBranch gb2) {
        return Comparator.comparing(GraphBranch::getBranchingTimestamp).compare(gb1, gb2);
    }

}
