package org.chronos.chronodb.internal.impl.dateback.log;

import org.chronos.chronodb.internal.api.dateback.log.IPurgeKeyspaceOperation;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class PurgeKeyspaceOperation extends AbstractDatebackOperation implements IPurgeKeyspaceOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String keyspace;
    private long fromTimestamp;
    private long toTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    public PurgeKeyspaceOperation(){
        // default constructor for (de-)serialization
    }

    public PurgeKeyspaceOperation(final String id, final String branch, final long wallClockTime, final String keyspace, final long fromTimestamp, final long toTimestamp){
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(fromTimestamp >= 0, "Precondition violation - argument 'fromTimestamp' must not be negative!");
        checkArgument(toTimestamp >= 0, "Precondition violation - argument 'toTimestamp' must not be negative!");
        this.keyspace = keyspace;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
    }

    public PurgeKeyspaceOperation(final String branch, final String keyspace, final long fromTimestamp, final long toTimestamp){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), keyspace, fromTimestamp, toTimestamp);
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
    public long getFromTimestamp() {
        return fromTimestamp;
    }

    @Override
    public long getToTimestamp() {
        return toTimestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PurgeKeyspaceOperation{");
        sb.append("keyspace='").append(keyspace).append('\'');
        sb.append(", fromTimestamp=").append(fromTimestamp);
        sb.append(", toTimestamp=").append(toTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
