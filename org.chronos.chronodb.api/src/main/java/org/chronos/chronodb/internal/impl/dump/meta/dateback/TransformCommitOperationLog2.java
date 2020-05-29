package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("TransformCommitOperation2")
public class TransformCommitOperationLog2 extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformCommitOperationLog2(){
        // default constructor for (de-)serialization
    }

    public TransformCommitOperationLog2(String id, String branch, long wallClockTime, long commitTimestamp) {
        super(id, branch, wallClockTime);
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        this.commitTimestamp = commitTimestamp;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "TransformCommit[target: " + this.getBranch() + "@" + this.getCommitTimestamp() + ", wallClockTime: " + this.getWallClockTime() + "]";
    }
}
