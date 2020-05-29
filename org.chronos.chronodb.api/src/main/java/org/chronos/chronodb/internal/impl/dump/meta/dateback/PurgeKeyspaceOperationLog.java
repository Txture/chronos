package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import static com.google.common.base.Preconditions.*;

public class PurgeKeyspaceOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String keyspace;
    private long fromTimestamp;
    private long toTimestamp;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    public PurgeKeyspaceOperationLog(){
        // default constructor for (de-)serialization
    }

    public PurgeKeyspaceOperationLog(String id, String branch, long wallClockTime, String keyspace, long fromTimestamp, long toTimestamp){
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(fromTimestamp >= 0, "Precondition violation - argument 'fromTimestamp' must not be negative!");
        checkArgument(toTimestamp >= 0, "Precondition violation - argument 'toTimestamp' must not be negative!");
        this.keyspace = keyspace;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public String getKeyspace() {
        return keyspace;
    }

    public long getFromTimestamp() {
        return fromTimestamp;
    }

    public long getToTimestamp() {
        return toTimestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PurgeKeyspaceOperationLog{");
        sb.append("keyspace='").append(keyspace).append('\'');
        sb.append(", fromTimestamp=").append(fromTimestamp);
        sb.append(", toTimestamp=").append(toTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
