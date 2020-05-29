package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("PurgeEntryOperation")
public class PurgeEntryOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long operationTimestamp;
    private String keyspace;
    private String key;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected PurgeEntryOperationLog(){
        // default constructor for (de-)serialization
    }

    public PurgeEntryOperationLog(String id, String branch, long wallClockTime, long operationTimestamp, String keyspace, String key){
        super(id, branch, wallClockTime);
        checkArgument(operationTimestamp >= 0, "Precondition violation - argument 'operationTimestamp' must not be negative!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.keyspace = keyspace;
        this.key = key;
        this.operationTimestamp = operationTimestamp;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public String getKey() {
        return key;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public long getOperationTimestamp() {
        return operationTimestamp;
    }

    @Override
    public String toString() {
        return "PurgeEntry[target: " + this.getBranch() + "@" + this.getOperationTimestamp() + ", wallClockTime: " + this.getWallClockTime() + ", purgedEntry: " + this.keyspace + "->" + this.key + "]";
    }
}
