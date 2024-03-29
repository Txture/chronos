package org.chronos.chronodb.internal.api;

import java.util.List;
import java.util.Set;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.internal.impl.IBranchMetadata;

/**
 * An extended version of the {@link BranchManager} interface.
 *
 * <p>
 * This interface and its methods are for internal use only, are subject to change and are not considered to be part of
 * the public API. Down-casting objects to internal interfaces may cause application code to become incompatible with
 * future releases, and is therefore strongly discouraged.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface BranchManagerInternal extends BranchManager {

	public void addBranchEventListener(BranchEventListener listener);

	public void removeBranchEventListener(BranchEventListener listener);

	@Override
	public BranchInternal getBranch(String branchName);

	/**
	 * Loads the given branches into the system.
	 *
	 * <p>
	 * It is <b>imperative</b> that the given list of branches is <b>sorted</b>. <b>Parent branches must always occur
	 * BEFORE child branches</b> in the list!
	 *
	 * <p>
	 * This method is intended for internal use only, when loading dump data.
	 *
	 * @param branches
	 *            The sorted list of branches, as indicated above. May be empty, but never <code>null</code>.
	 */
	public void loadBranchDataFromDump(List<IBranchMetadata> branches);

	public default long getMaxNowAcrossAllBranches() {
		return this.getBranches().stream().mapToLong(Branch::getNow).max().orElse(0);
	}

}
