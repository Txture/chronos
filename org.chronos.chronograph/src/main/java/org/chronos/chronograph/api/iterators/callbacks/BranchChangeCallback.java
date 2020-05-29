package org.chronos.chronograph.api.iterators.callbacks;

@FunctionalInterface
public interface BranchChangeCallback {

    public static final BranchChangeCallback IGNORE = (previousBranch, nextBranch) -> {
    };

    public void handleBranchChanged(String previousBranch, String nextBranch);

}
