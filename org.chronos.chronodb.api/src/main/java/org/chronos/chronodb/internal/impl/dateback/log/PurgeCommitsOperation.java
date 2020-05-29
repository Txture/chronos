package org.chronos.chronodb.internal.impl.dateback.log;

import com.google.common.collect.Sets;
import org.chronos.chronodb.internal.api.dateback.log.IPurgeCommitsOperation;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class PurgeCommitsOperation extends AbstractDatebackOperation implements IPurgeCommitsOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private Set<Long> commitTimestamps;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected PurgeCommitsOperation(){
        // default constructor for (de-)serialization
    }

    public PurgeCommitsOperation(String id, String branch, long wallClockTime, Set<Long> commitTimestamps){
        super(id, branch, wallClockTime);
        checkNotNull(commitTimestamps, "Precondition violation - argument 'commitTimestamps' must not be NULL!");
        commitTimestamps.forEach(t ->  checkArgument(t >= 0, "Precondition violation - argument 'commitTimestamps' must not contain negative values!"));
        this.commitTimestamps = Sets.newHashSet(commitTimestamps);
    }

    public PurgeCommitsOperation(String branch, Set<Long> commitTimestamps){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), commitTimestamps);
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
        return timestamp >= this.commitTimestamps.stream().mapToLong(t -> t).min().orElse(Long.MAX_VALUE);
    }

    @Override
    public Set<Long> getCommitTimestamps() {
        return Collections.unmodifiableSet(this.commitTimestamps);
    }

    @Override
    public String toString() {
        return "PurgeCommits[branch: " + this.getBranch() + ", wallClockTime: " + this.getWallClockTime() + ", timestamps: " + this.commitTimestamps+ "]";
    }
}
