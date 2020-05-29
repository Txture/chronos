package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.google.common.collect.Sets;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.chronos.chronodb.internal.impl.dateback.log.AbstractDatebackOperation;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("PurgecommitsOperation")
public class PurgeCommitsOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private Set<Long> commitTimestamps;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected PurgeCommitsOperationLog(){
        // default constructor for (de-)serialization
    }

    public PurgeCommitsOperationLog(String id, String branch, long wallClockTime, Set<Long> commitTimestamps){
        super(id, branch, wallClockTime);
        checkNotNull(commitTimestamps, "Precondition violation - argument 'commitTimestamps' must not be NULL!");
        commitTimestamps.forEach(t ->  checkArgument(t >= 0, "Precondition violation - argument 'commitTimestamps' must not contain negative values!"));
        this.commitTimestamps = Sets.newHashSet(commitTimestamps);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public Set<Long> getCommitTimestamps() {
        return Collections.unmodifiableSet(this.commitTimestamps);
    }

    @Override
    public String toString() {
        return "PurgeCommits[branch: " + this.getBranch() + ", wallClockTime: " + this.getWallClockTime() + ", timestamps: " + this.commitTimestamps+ "]";
    }
}
