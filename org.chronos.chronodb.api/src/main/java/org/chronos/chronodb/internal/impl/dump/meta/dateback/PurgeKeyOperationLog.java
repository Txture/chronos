package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.dateback.log.AbstractDatebackOperation;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("PurgeKeyOperation")
public class PurgeKeyOperationLog extends DatebackLog {

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

    public PurgeKeyOperationLog(){
        // default constructor for (de-)serialization
    }

    public PurgeKeyOperationLog(String id, String branch, long wallClockTime, String keyspace, String key, long fromTimestamp, long toTimestamp){
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

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public String getKeyspace() {
        return keyspace;
    }

    public String getKey() {
        return key;
    }

    public long getFromTimestamp() {
        return fromTimestamp;
    }

    public long getToTimestamp() {
        return toTimestamp;
    }

    @Override
    public String toString() {
        return "PurgeKey[branch: " + this.getBranch() + ", wallClockTime: " + this.getWallClockTime() + ", purged: " + this.keyspace + "->" + this.key + ", period: " + Period.createRange(this.fromTimestamp, this.toTimestamp+1) + "]";
    }
}
