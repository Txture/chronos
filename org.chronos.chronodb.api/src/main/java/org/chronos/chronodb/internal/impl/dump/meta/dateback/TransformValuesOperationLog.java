package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.google.common.collect.Sets;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.chronos.chronodb.internal.api.Period;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("TransformValuesOperation")
public class TransformValuesOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private Set<Long> commitTimestamps;
    private String keyspace;
    private String key;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformValuesOperationLog() {
        // default constructor for (de-)serialization
    }

    public TransformValuesOperationLog(String id, String branch, long wallClockTime, String keyspace, String key, Set<Long> commitTimestamps) {
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(commitTimestamps, "Precondition violation - argument 'commitTimestamps' must not be NULL!");
        checkArgument(commitTimestamps.stream().anyMatch(t -> t < 0), "Precondition violation - none of the provided 'commitTimestamps' must be negative!");
        this.keyspace = keyspace;
        this.key = key;
        this.commitTimestamps = Sets.newHashSet(commitTimestamps);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public Set<Long> getCommitTimestamps() {
        return Collections.unmodifiableSet(commitTimestamps);
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        long min = this.commitTimestamps.stream().mapToLong(l -> l).min().orElseThrow(IllegalStateException::new);
        long max = this.commitTimestamps.stream().mapToLong(l -> l).max().orElseThrow(IllegalStateException::new);
        if (max < Long.MAX_VALUE) {
            max += 1; // ranges render upper bound as exclusive, but our internal notation is inclusive
        }
        return "TransformValues[target: " + this.getBranch() + ", key: " + this.keyspace + "->" + this.key + " in " + this.commitTimestamps.size() + " timestamps in range " + Period.createRange(min, max) + "]";
    }
}
