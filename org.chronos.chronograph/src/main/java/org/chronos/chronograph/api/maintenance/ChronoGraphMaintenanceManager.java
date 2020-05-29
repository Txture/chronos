package org.chronos.chronograph.api.maintenance;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.internal.impl.structure.graph.features.ChronoGraphGraphFeatures;

import java.util.function.Predicate;

public interface ChronoGraphMaintenanceManager {

    /**
     * Performs a rollover on the branch with the given name.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * @param branchName
     *            The branch name to roll over. Must not be <code>null</code>, must refer to an existing branch.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public default void performRolloverOnBranch(String branchName) {
        this.performRolloverOnBranch(branchName, true);
    }

    /**
     * Performs a rollover on the branch with the given name.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * @param branchName
     *            The branch name to roll over. Must not be <code>null</code>, must refer to an existing branch.
     * @param updateIndex
     *            Use <code>true</code> to update all clean indices to match the data content at the new head revision. Using <code>false</code> will instead mark all indices as dirty.
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public void performRolloverOnBranch(String branchName, boolean updateIndex);

    /**
     * Performs a rollover on the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public default void performRolloverOnMaster() {
        this.performRolloverOnBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }


    /**
     * Performs a rollover on the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * @param updateIndex
     *            Use <code>true</code> to update all clean indices to match the data content at the new head revision. Using <code>false</code> will instead mark all indices as dirty.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public default void performRolloverOnMaster(boolean updateIndex){
        this.performRolloverOnBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, updateIndex);
    }

    /**
     * Performs a rollover on all existing branches.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * <p>
     * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
     *
     * <p>
     * This is a <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public default void performRolloverOnAllBranches(){
        this.performRolloverOnAllBranches(true);
    }

    /**
     * Performs a rollover on all existing branches.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * <p>
     * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
     *
     * <p>
     * This is a <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
     *
     * @param updateIndex
     *            Use <code>true</code> to update all clean indices to match the data content at the new head revision. Using <code>false</code> will instead mark all indices as dirty.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public void performRolloverOnAllBranches(boolean updateIndex);

    /**
     * Performs a rollover on all branches that match the given predicate.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * <p>
     * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
     *
     * <p>
     * This is a potentially <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
     *
     * @param branchPredicate
     *            The predicate that decides whether or not to roll over the branch in question. Must not be <code>null</code>.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public default void performRolloverOnAllBranchesWhere(Predicate<String> branchPredicate){
        this.performRolloverOnAllBranchesWhere(branchPredicate, true);
    }

    /**
     * Performs a rollover on all branches that match the given predicate.
     *
     * <p>
     * Not all backends support this operation. Please use {@link ChronoGraphGraphFeatures#supportsRollover()} first to check if this operation is supported or not.
     *
     * <p>
     * <b>Important note:</b> This method is <b>not guaranteed to be ACID safe</b>. Rollovers will be executed one after the other. If an unexpected event (such as JVM crash, exceptions, power supply failure...) occurs, then some branches may have been rolled over while others have not.
     *
     * <p>
     * This is a potentially <b>very</b> expensive operation that can substantially increase the memory footprint of the database on disk. Use with care.
     *
     * @param branchPredicate
     *            The predicate that decides whether or not to roll over the branch in question. Must not be <code>null</code>.
     *
     * @param updateIndex
     *            Use <code>true</code> to update all clean indices to match the data content at the new head revision. Using <code>false</code> will instead mark all indices as dirty.
     *
     * @throws UnsupportedOperationException
     *             Thrown if this backend {@linkplain ChronoGraphGraphFeatures#supportsRollover() does not support rollovers}.
     */
    public void performRolloverOnAllBranchesWhere(Predicate<String> branchPredicate, boolean updateIndex);

}
