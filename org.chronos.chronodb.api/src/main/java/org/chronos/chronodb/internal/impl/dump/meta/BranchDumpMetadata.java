package org.chronos.chronodb.internal.impl.dump.meta;

import static com.google.common.base.Preconditions.*;

import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDBConstants;

public class BranchDumpMetadata {

	public static BranchDumpMetadata createMasterBranchMetadata() {
		BranchDumpMetadata metadata = new BranchDumpMetadata();
		metadata.name = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER;
		metadata.branchingTimestamp = 0L;
		metadata.parentName = null;
		return metadata;
	}

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private String name;
	private String parentName;
	private long branchingTimestamp;
	private String directoryName;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	public BranchDumpMetadata() {
		// serialization constructor
	}

	public BranchDumpMetadata(final Branch branch) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		this.name = branch.getName();
		if (branch.getOrigin() != null) {
			this.parentName = branch.getOrigin().getName();
		} else {
			this.parentName = null;
		}
		this.branchingTimestamp = branch.getBranchingTimestamp();
		this.directoryName = branch.getDirectoryName();
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	public String getName() {
		return this.name;
	}

	public String getParentName() {
		return this.parentName;
	}

	public long getBranchingTimestamp() {
		return this.branchingTimestamp;
	}

	public String getDirectoryName(){
		return this.directoryName;
	}

	@Override
	public String toString() {
		return "BranchDumpMetadata["  +this.name + ", parent=" + this.parentName + ", branchingTimestamp=" + branchingTimestamp + ", dirName=" + directoryName + "]";
	}
}
