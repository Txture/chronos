package org.chronos.chronodb.internal.impl.dateback.log;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class TransformValuesOfKeyspaceOperation extends AbstractDatebackOperation{

    private String branch;
    private String keyspace;
    private long minChangedTimestamp;

    private TransformValuesOfKeyspaceOperation(){
        // default constructor for deserialization
    }

    public TransformValuesOfKeyspaceOperation(final String id, final String branch, final long wallClockTime, final String keyspace, final long minChangedTimestamp){
        super(id, branch, wallClockTime);
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(minChangedTimestamp >= 0, "Preconditino violation - argument 'minChangedTimestamp' must not be negative!");
        this.branch = branch;
        this.keyspace = keyspace;
        this.minChangedTimestamp = minChangedTimestamp;
    }

    public TransformValuesOfKeyspaceOperation(final String branch, final String keyspace, final long minChangedTimestamp) {
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), keyspace, minChangedTimestamp);
    }

    public String getBranch() {
        return branch;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return this.minChangedTimestamp <= timestamp;
    }

    @Override
    public long getEarliestAffectedTimestamp() {
        return this.minChangedTimestamp;
    }

    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TransformValuesOfKeyspaceOperation{");
        sb.append("branch='").append(branch).append('\'');
        sb.append(", keyspace='").append(keyspace).append('\'');
        sb.append(", minChangedTimestamp=").append(minChangedTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
