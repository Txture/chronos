package org.chronos.chronodb.internal.impl.dateback.log;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.dateback.log.IPurgeKeyOperation;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class PurgeKeyOperation extends AbstractDatebackOperation implements IPurgeKeyOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String keyspace;
    private String key;
    private long fromTimestamp;
    private long toTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    public PurgeKeyOperation(){
        // default constructor for (de-)serialization
    }

    public PurgeKeyOperation(String id, String branch, long wallClockTime, String keyspace, String key, long fromTimestamp, long toTimestamp){
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(fromTimestamp >= 0, "Precondition violation - argument 'fromTimestamp' must not be negative!");
        checkArgument(toTimestamp >= 0, "Precondition violation - argument 'toTimestamp' must not be negative!");
        this.keyspace = keyspace;
        this.key = key;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
    }

    public PurgeKeyOperation(String branch, String keyspace, String key, long fromTimestamp, long toTimestamp){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), keyspace, key, fromTimestamp, toTimestamp);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        return this.fromTimestamp;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return timestamp >= this.fromTimestamp;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public long getFromTimestamp() {
        return fromTimestamp;
    }

    @Override
    public long getToTimestamp() {
        return toTimestamp;
    }

    @Override
    public String toString() {
        return "PurgeKey[branch: " + this.getBranch() + ", wallClockTime: " + this.getWallClockTime() + ", purged: " + this.keyspace + "->" + this.key + ", period: " + Period.createRange(this.fromTimestamp, this.toTimestamp+1) + "]";
    }
}
