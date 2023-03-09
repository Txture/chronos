package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.thoughtworks.xstream.annotations.XStreamAlias;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("TransformValuesOfKeyspaceOperation")
public class TransformValuesOfKeyspaceOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String keyspace;
    private long earliestAffectedCommit;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformValuesOfKeyspaceOperationLog() {
        // default constructor for (de-)serialization
    }

    public TransformValuesOfKeyspaceOperationLog(String id, String branch, long wallClockTime, String keyspace, long earliestAffectedCommit) {
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(earliestAffectedCommit > 0, "Precondition violation - argument 'earliestAffectedCommit' must not be negative!");
        this.keyspace = keyspace;
        this.earliestAffectedCommit = earliestAffectedCommit;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    public long getEarliestAffectedCommit() {
        return earliestAffectedCommit;
    }

    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String toString() {
        return "TransformValuesOfKeyspace[target: " + this.getBranch()
            + ", keyspace: " + this.keyspace
            + ", earliest affected commit: "
            + this.earliestAffectedCommit
            + "]";
    }
}
