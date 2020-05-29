package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("UpdateCommitMetadataOperation")
public class UpdateCommitMetadataOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected UpdateCommitMetadataOperationLog(){
        // default constructor for (de-)serialization
    }

    public UpdateCommitMetadataOperationLog(String id, String branch, long wallClockTime, long commitTimestamp){
        super(id, branch, wallClockTime);
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
        return "UpdateCommitMetadata[target: " + this.getBranch() + "@" + this.commitTimestamp + ", wallClockTime: " + this.getWallClockTime() + "]";
    }

}
