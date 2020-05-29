package org.chronos.chronodb.internal.impl.dateback.log.v2;

import org.chronos.chronodb.internal.api.dateback.log.ITransformCommitOperation;
import org.chronos.chronodb.internal.impl.dateback.log.AbstractDatebackOperation;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class TransformCommitOperation2 extends AbstractDatebackOperation implements ITransformCommitOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformCommitOperation2(){
        // default constructor for (de-)serialization
    }

    public TransformCommitOperation2(String id, String branch, long wallClockTime, long commitTimestamp) {
        super(id, branch, wallClockTime);
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        this.commitTimestamp = commitTimestamp;
    }


    public TransformCommitOperation2(String branch, long commitTimestamp) {
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
        return timestamp >= this.commitTimestamp;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "TransformCommit[target: " + this.getBranch() + "@" + this.getCommitTimestamp() + ", wallClockTime: " + this.getWallClockTime() + "]";
    }
}