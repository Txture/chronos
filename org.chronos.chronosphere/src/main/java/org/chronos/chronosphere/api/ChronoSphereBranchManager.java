package org.chronos.chronosphere.api;

import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;

/**
 * The branch manager is responsible for creating and maintaining branches in a {@link ChronoSphere} repository.
 *
 * <p>
 * You can get an instance of this class via {@link ChronoSphere#getBranchManager()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoSphereBranchManager {

	/**
	 * Creates a new child of the master branch with the given name.
	 *
	 * <p>
	 * This will use the head revision as the base revision for the new branch.
	 *
	 * @param branchName
	 *            The name of the new branch. Must not be <code>null</code>. Must not refer to an already existing
	 *            branch. Branch names must be unique.
	 *
	 * @return The newly created branch. Never <code>null</code>.
	 *
	 * @see #createBranch(String, long)
	 * @see #createBranch(String, String)
	 * @see #createBranch(String, String, long)
	 */
	public SphereBranch createBranch(String branchName);

	/**
	 * Creates a new child of the master branch with the given name.
	 *
	 * @param branchName
	 *            The name of the new branch. Must not be <code>null</code>. Must not refer to an already existing
	 *            branch. Branch names must be unique.
	 * @param branchingTimestamp
	 *            The timestamp at which to branch away from the master branch. Must not be negative. Must be less than
	 *            or equal to the timestamp of the latest commit on the master branch.
	 *
	 * @return The newly created branch. Never <code>null</code>.
	 *
	 * @see #createBranch(String)
	 * @see #createBranch(String, String)
	 * @see #createBranch(String, String, long)
	 */
	public SphereBranch createBranch(String branchName, long branchingTimestamp);

	/**
	 * Creates a new child of the given parent branch with the given name.
	 *
	 * <p>
	 * This will use the head revision of the given parent branch as the base revision for the new branch.
	 *
	 * @param parentName
	 *            The name of the parent branch. Must not be <code>null</code>. Must refer to an existing branch.
	 * @param newBranchName
	 *            The name of the new child branch. Must not be <code>null</code>. Must not refere to an already
	 *            existing branch. Branch names must be unique.
	 *
	 * @return The newly created branch. Never <code>null</code>.
	 *
	 * @see #createBranch(String)
	 * @see #createBranch(String, long)
	 * @see #createBranch(String, String, long)
	 */
	public SphereBranch createBranch(String parentName, String newBranchName);

	/**
	 * Creates a new child of the given parent branch with the given name.
	 *
	 * @param parentName
	 *            The name of the parent branch. Must not be <code>null</code>. Must refer to an existing branch.
	 * @param newBranchName
	 *            The name of the new child branch. Must not be <code>null</code>. Must not refere to an already
	 *            existing branch. Branch names must be unique.
	 * @param branchingTimestamp
	 *            The timestamp at which to branch away from the parent branch. Must not be negative. Must be less than
	 *            or equal to the timestamp of the latest commit on the parent branch.
	 *
	 * @return The newly created branch. Never <code>null</code>.
	 *
	 * @see #createBranch(String)
	 * @see #createBranch(String, long)
	 * @see #createBranch(String, String, long)
	 */
	public SphereBranch createBranch(String parentName, String newBranchName, long branchingTimestamp);

	/**
	 * Checks if a branch with the given name exists or not.
	 *
	 * @param branchName
	 *            The branch name to check. Must not be <code>null</code>.
	 * @return <code>true</code> if there is an existing branch with the given name, otherwise <code>false</code>.
	 */
	public boolean existsBranch(String branchName);

	/**
	 * Returns the branch with the given name.
	 *
	 * @param branchName
	 *            The name of the branch to retrieve. Must not be <code>null</code>. Must refer to an existing branch.
	 *
	 * @return The branch with the given name. Never <code>null</code>.
	 */
	public SphereBranch getBranch(String branchName);

	/**
	 * Returns the master branch.
	 *
	 * @return The master branch. Never <code>null</code>.
	 */
	public default SphereBranch getMasterBranch() {
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
	public Set<SphereBranch> getBranches();

}
