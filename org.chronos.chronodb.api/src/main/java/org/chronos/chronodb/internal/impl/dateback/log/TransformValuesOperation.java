package org.chronos.chronodb.internal.impl.dateback.log;

import com.google.common.collect.Sets;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.dateback.log.ITransformValuesOperation;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class TransformValuesOperation extends AbstractDatebackOperation implements ITransformValuesOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private Set<Long> commitTimestamps;
    private String keyspace;
    private String key;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformValuesOperation() {
        // default constructor for (de-)serialization
    }

    public TransformValuesOperation(String id, String branch, long wallClockTime, String keyspace, String key, Set<Long> commitTimestamps) {
        super(id, branch, wallClockTime);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(commitTimestamps, "Precondition violation - argument 'commitTimestamps' must not be NULL!");
        checkArgument(commitTimestamps.stream().anyMatch(t -> t < 0), "Precondition violation - none of the provided 'commitTimestamps' must be negative!");
        this.keyspace = keyspace;
        this.key = key;
        this.commitTimestamps = Sets.newHashSet(commitTimestamps);
    }


    public TransformValuesOperation(String branch, String keyspace, String key, Set<Long> commitTimestamps) {
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), keyspace, key, commitTimestamps);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        // if we don't have any entries, the operation affects no timestamps at all -> we return MAX_VALUE (= infinity).
        return this.commitTimestamps.stream().mapToLong(t->t).min().orElse(Long.MAX_VALUE);
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return this.commitTimestamps.stream().mapToLong(t -> t).min().orElseThrow(IllegalStateException::new) <= timestamp;
    }

    @Override
    public Set<Long> getCommitTimestamps() {
        return Collections.unmodifiableSet(commitTimestamps);
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
    public String toString() {
        long min = this.commitTimestamps.stream().mapToLong(l -> l).min().orElseThrow(IllegalStateException::new);
        long max = this.commitTimestamps.stream().mapToLong(l -> l).max().orElseThrow(IllegalStateException::new);
        if (max < Long.MAX_VALUE) {
            max += 1; // ranges render upper bound as exclusive, but our internal notation is inclusive
        }
        return "TransformValues[target: " + this.getBranch() + ", key: " + this.keyspace + "->" + this.key + " in " + this.commitTimestamps.size() + " timestamps in range " + Period.createRange(min, max) + "]";
    }
}
