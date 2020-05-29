package org.chronos.chronodb.internal.impl.engines.base;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.IBranchMetadata;

import com.google.common.collect.Sets;
import org.chronos.common.autolock.AutoLock;

public abstract class AbstractBranchManager implements BranchManager, BranchManagerInternal {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	protected final BranchDirectoryNameResolver dirNameResolver;

	// =================================================================================================================
	// CONSTRUCTOR
	// =================================================================================================================

	protected AbstractBranchManager(final BranchDirectoryNameResolver branchDirectoryNameResolver) {
		checkNotNull(branchDirectoryNameResolver, "Precondition violation - argument 'branchDirectoryNameResolver' must not be NULL!");
		this.dirNameResolver = branchDirectoryNameResolver;
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public Branch createBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, false);
		long now = this.getMasterBranch().getTemporalKeyValueStore().getNow();
		return this.createBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, branchName, now);
	}

	@Override
	public Branch createBranch(final String branchName, final long branchingTimestamp) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, false);
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		long now = this.getMasterBranch().getTemporalKeyValueStore().getNow();
		checkArgument(branchingTimestamp <= now,
				"Precondition violation - argument 'branchingTimestamp' must be less than the timestamp of the latest commit on the parent branch!");
		return this.createBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, branchName, branchingTimestamp);
	}

	@Override
	public Branch createBranch(final String parentName, final String newBranchName) {
		checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		checkNotNull(newBranchName, "Precondition violation - argument 'newBranchName' must not be NULL!");
		this.assertBranchNameExists(newBranchName, false);
		this.assertBranchNameExists(parentName, true);
		long now = this.getBranchInternal(parentName).getTemporalKeyValueStore().getNow();
		return this.createBranch(parentName, newBranchName, now);
	}

	@Override
	public Branch createBranch(final String parentName, final String newBranchName, final long branchingTimestamp) {
		checkNotNull(parentName, "Precondition violation - argument 'parentName' must not be NULL!");
		checkNotNull(newBranchName, "Precondition violation - argument 'newBranchName' must not be NULL!");
		this.assertBranchNameExists(parentName, true);
		this.assertBranchNameExists(newBranchName, false);
		checkArgument(branchingTimestamp >= 0,
				"Precondition violation - argument 'branchingTimestamp' must not be negative!");
		long now = this.getBranchInternal(parentName).getTemporalKeyValueStore().getNow();
		checkArgument(branchingTimestamp <= now,
				"Precondition violation - argument 'branchingTimestamp' must be less than the timestamp of the latest commit on the parent branch!");
		String directoryName = this.dirNameResolver.createDirectoryNameForBranchName(newBranchName);
		IBranchMetadata metadata = IBranchMetadata.create(newBranchName, parentName, branchingTimestamp, directoryName);
		return this.createBranch(metadata);
	}

	@Override
	public boolean existsBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		return this.getBranchInternal(branchName) != null;
	}

	@Override
	public BranchInternal getBranch(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		this.assertBranchNameExists(branchName, true);
		return this.getBranchInternal(branchName);
	}

	@Override
	public Set<Branch> getBranches() {
		Set<Branch> resultSet = Sets.newHashSet();
		for (String branchName : this.getBranchNames()) {
			Branch branch = this.getBranch(branchName);
			resultSet.add(branch);
		}
		return Collections.unmodifiableSet(resultSet);
	}

	@Override
	public List<String> deleteBranchRecursively(final String branchName) {
		checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
		checkArgument(!branchName.equals(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER), "Precondition violation - the master branch cannot be deleted!");
		try(AutoLock lock = this.getOwningDB().lockExclusive()){
			BranchInternal branchToDelete = this.getBranch(branchName);
			if(branchToDelete == null){
				// there is no branch to delete
				return Collections.emptyList();
			}
			// find the child branches, recursively
			Set<Branch> allBranches = this.getBranches();
			List<String> branchesToDelete = Lists.newArrayList();
			branchesToDelete.add(branchToDelete.getName());
			boolean changed = true;
			while(changed){
				changed = false;
				for(Branch branch : allBranches){
					if(branchesToDelete.contains(branch.getMetadata().getParentName()) && !branchesToDelete.contains(branch.getName())){
						// the parent branch is supposed to be deleted; delete this one as well
						branchesToDelete.add(branch.getName());
						changed = true;
					}
				}
			}
			List<String> deletedBranches = Lists.newArrayList();
			// start deleting in the child branch
			for(String branchNameToDelete : Lists.reverse(branchesToDelete)) {
				this.deleteSingleBranch(this.getBranch(branchNameToDelete));
				deletedBranches.add(branchNameToDelete);
			}
			return Collections.unmodifiableList(deletedBranches);
		}
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	@Override
	public void loadBranchDataFromDump(final List<IBranchMetadata> branches) {
		checkNotNull(branches, "Precondition violation - argument 'branches' must not be NULL!");
		for (IBranchMetadata branchMetadata : branches) {
			// assert that the branch does not yet exist
			if (this.existsBranch(branchMetadata.getName())) {
				throw new IllegalStateException(
						"There already exists a branch named '" + branchMetadata.getName() + "'!");
			}
			// assert that the parent branch does exist
			if (this.existsBranch(branchMetadata.getParentName()) == false) {
				throw new IllegalStateException(
						"Attempted to create branch '" + branchMetadata.getName() + "' on parent '"
								+ branchMetadata.getParentName() + "', but the parent branch does not exist!");
			}
			this.createBranch(branchMetadata);
		}
	}

	protected abstract void deleteSingleBranch(Branch branch);

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	@Override
	public BranchInternal getMasterBranch() {
		// this override is convenient because it returns the BranchInternal representation of the branch.
		return this.getBranchInternal(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected void assertBranchNameExists(final String branchName, final boolean exists) {
		if (exists) {
			if (this.existsBranch(branchName) == false) {
				throw new ChronoDBBranchingException("There is no branch named '" + branchName + "'!");
			}
		} else {
			if (this.existsBranch(branchName)) {
				throw new ChronoDBBranchingException("A branch with name '" + branchName + "' already exists!");
			}
		}
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	protected abstract BranchInternal createBranch(IBranchMetadata metadata);

	protected abstract BranchInternal getBranchInternal(final String name);

	protected abstract ChronoDBInternal getOwningDB();
}
