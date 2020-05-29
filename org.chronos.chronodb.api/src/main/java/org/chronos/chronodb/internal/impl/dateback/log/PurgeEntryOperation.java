package org.chronos.chronodb.internal.impl.dateback.log;

import org.chronos.chronodb.internal.api.dateback.log.IPurgeEntryOperation;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class PurgeEntryOperation extends AbstractDatebackOperation implements IPurgeEntryOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long operationTimestamp;
    private String keyspace;
    private String key;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected PurgeEntryOperation() {
        // default constructor for (de-)serialization
    }

    public PurgeEntryOperation(String id, String branch, long wallClockTime, long operationTimestamp, String keyspace, String key) {
        super(id, branch, wallClockTime);
        checkArgument(operationTimestamp >= 0, "Precondition violation - argument 'operationTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.keyspace = keyspace;
        this.key = key;
        this.operationTimestamp = operationTimestamp;
    }

    public PurgeEntryOperation(String branch, long operationTimestamp, String keyspace, String key) {
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), operationTimestamp, keyspace, key);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        return this.operationTimestamp;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return timestamp >= this.operationTimestamp;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public String toString() {
        return "PurgeEntry[target: " + this.getBranch() + "@" + this.getOperationTimestamp() + ", wallClockTime: " + this.getWallClockTime() + ", purgedEntry: " + this.keyspace + "->" + this.key + "]";
    }
}
