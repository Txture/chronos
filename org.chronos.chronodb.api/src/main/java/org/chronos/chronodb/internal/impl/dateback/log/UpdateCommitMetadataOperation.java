package org.chronos.chronodb.internal.impl.dateback.log;

import org.chronos.chronodb.internal.api.dateback.log.IUpdateCommitMetadataOperation;

import java.util.UUID;

public class UpdateCommitMetadataOperation extends AbstractDatebackOperation implements IUpdateCommitMetadataOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected UpdateCommitMetadataOperation(){
        // default constructor for (de-)serialization
    }

    public UpdateCommitMetadataOperation(String id, String branch, long wallClockTime, long commitTimestamp){
        super(id, branch, wallClockTime);
        this.commitTimestamp = commitTimestamp;
    }

    public UpdateCommitMetadataOperation(String branch, long commitTimestamp){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), commitTimestamp);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        return this.commitTimestamp;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        // changes on the commit metadata only affect the commit itself, not the future
        return this.commitTimestamp == timestamp;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "UpdateCommitMetadata[target: " + this.getBranch() + "@" + this.commitTimestamp + ", wallClockTime: " + this.getWallClockTime() + "]";
    }

}
