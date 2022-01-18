package org.chronos.chronodb.api;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

/**
 * The {@link BranchManager} is responsible for managing the branching functionality inside a {@link ChronoDB}.
 *
 * <p>
 * By default, every ChronoDB instance contains at least one branch, the master branch. Any other branches are children of the master branch.
 *
 * <p>
 * <b>All operations on the branch manager are considered to be management operations, and therefore are by default neither versioned nor ACID protected.</b>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface BranchManager {

    /**
     * Creates a new child of the master branch with the given name.
     *
     * <p>
     * This will use the head revision as the base revision for the new branch.
     *
     * @param branchName The name of the new branch. Must not be <code>null</code>. Must not refer to an already existing branch. Branch names must be unique.
     * @return The newly created branch. Never <code>null</code>.
     * @see #createBranch(String, long)
     * @see #createBranch(String, String)
     * @see #createBranch(String, String, long)
     */
    public Branch createBranch(String branchName);

    /**
     * Creates a new child of the master branch with the given name.
     *
     * @param branchName         The name of the new branch. Must not be <code>null</code>. Must not refer to an already existing branch. Branch names must be unique.
     * @param branchingTimestamp The timestamp at which to branch away from the master branch. Must not be negative. Must be less than or equal to the timestamp of the latest commit on the master branch.
     * @return The newly created branch. Never <code>null</code>.
     * @see #createBranch(String)
     * @see #createBranch(String, String)
     * @see #createBranch(String, String, long)
     */
    public Branch createBranch(String branchName, long branchingTimestamp);

    /**
     * Creates a new child of the given parent branch with the given name.
     *
     * <p>
     * This will use the head revision of the given parent branch as the base revision for the new branch.
     *
     * @param parentName    The name of the parent branch. Must not be <code>null</code>. Must refer to an existing branch.
     * @param newBranchName The name of the new child branch. Must not be <code>null</code>. Must not refere to an already existing branch. Branch names must be unique.
     * @return The newly created branch. Never <code>null</code>.
     * @see #createBranch(String)
     * @see #createBranch(String, long)
     * @see #createBranch(String, String, long)
     */
    public Branch createBranch(String parentName, String newBranchName);

    /**
     * Creates a new child of the given parent branch with the given name.
     *
     * @param parentName         The name of the parent branch. Must not be <code>null</code>. Must refer to an existing branch.
     * @param newBranchName      The name of the new child branch. Must not be <code>null</code>. Must not refere to an already existing branch. Branch names must be unique.
     * @param branchingTimestamp The timestamp at which to branch away from the parent branch. Must not be negative. Must be less than or equal to the timestamp of the latest commit on the parent branch.
     * @return The newly created branch. Never <code>null</code>.
     * @see #createBranch(String)
     * @see #createBranch(String, long)
     * @see #createBranch(String, String, long)
     */
    public Branch createBranch(String parentName, String newBranchName, long branchingTimestamp);

    /**
     * Checks if a branch with the given name exists or not.
     *
     * @param branchName The branch name to check. Must not be <code>null</code>.
     * @return <code>true</code> if there is an existing branch with the given name, otherwise <code>false</code>.
     */
    public boolean existsBranch(String branchName);

    /**
     * Returns the branch with the given name.
     *
     * @param branchName The name of the branch to retrieve. Must not be <code>null</code>. Must refer to an existing branch.
     * @return The branch with the given name, or <code>null</code> if there is no branch with that name.
     */
    public Branch getBranch(String branchName);

    /**
     * Returns the master branch.
     *
     * @return The master branch. Never <code>null</code>.
     */
    public default Branch getMasterBranch() {
        return this.getBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    /**
     * Returns the name of all existing branches.
     *
     * @return An unmodifiable view on the names of all branches. May be empty, but never <code>null</code>.
     */
    public Set<String> getBranchNames();

    /**
     * Returns the set of all existing branches.
     *
     * @return An unmodifiable view on the set of all branches. May be empty, but never <code>null</code>.
     */
    public Set<Branch> getBranches();

    public List<Branch> getChildBranches(Branch branch, boolean recursive);

    /**
     * Deletes the branch with the given name, and its child branches (recursively).
     *
     * <p>
     * <b>/!\ WARNING /!\</b><br>
     * This is a <i>management operation</i> which <b>should not be used</b> while the database is under
     * active use! The behaviour for all current and future transactions on the given branch is <b>undefined</b>!
     * This operation is <b>not atomic</b>: if a child branch is deleted and an error occurs, the parent branch may still continue to exist.
     * Child branches are deleted before parent branches.
     * </p>
     *
     * @param branchName The name of the branch to delete. Must not be <code>null</code>. The {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch cannot be deleted!
     * @return The list of branch names which have been deleted by this operation. May be empty, but never <code>null</code>.
     */
    public List<String> deleteBranchRecursively(String branchName);

    /**
     * Gets the actual branch which is referred to by the given coordinates.
     *
     * <p>
     * If a query is executed on a (non-master) branch, and the query timestamp is
     * before the branching timestamp, the actual branch that is being queried is
     * the parent branch (maybe recursively).
     * </p>
     *
     * @param branchName The name of the target branch. Must refer to an existing branch. Must not be <code>null</code>.
     * @param timestamp  The timestamp to resolve. Must not be negative.
     * @return The actual branch which will be affected by the query. This branch may either be the same as,
     * or a direct or indirect parent of the given branch
     */
    public Branch getActualBranchForQuerying(@NotNull final String branchName, final long timestamp);

    /**
     * Gets the actual branch which is referred to by the given coordinates.
     *
     * <p>
     * If a query is executed on a (non-master) branch, and the query timestamp is
     * before the branching timestamp, the actual branch that is being queried is
     * the parent branch (maybe recursively).
     * </p>
     *
     * @param branch    The target branch. Must not be <code>null</code>.
     * @param timestamp The timestamp to resolve. Must not be negative.
     * @return The actual branch which will be affected by the query. This branch may either be the same as,
     * or a direct or indirect parent of the given branch
     */
    public default Branch getActualBranchForQuerying(@NotNull final Branch branch, final long timestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.getActualBranchForQuerying(branch.getName(), timestamp);
    }

}
