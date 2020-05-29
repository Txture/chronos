package org.chronos.chronodb.internal.impl.dateback.log;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.dateback.log.ITransformCommitOperation;
import org.chronos.chronodb.internal.impl.dateback.log.v2.TransformCommitOperation2;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.*;

/**
 * This class has been deprecated in favour of {@link TransformCommitOperation2}.
 *
 * <p>
 * We still keep it here for backwards compatibility / deserialization purposes.
 * </p>
 */
@Deprecated
public class TransformCommitOperation extends AbstractDatebackOperation implements ITransformCommitOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;
    private Set<QualifiedKey> changedKeys;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformCommitOperation(){
        // default constructor for (de-)serialization
    }

    public TransformCommitOperation(String id, String branch, long wallClockTime, long commitTimestamp, Set<QualifiedKey> changedKeys) {
        super(id, branch, wallClockTime);
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(changedKeys, "Precondition violation - argument 'changedKeys' must not be NULL!");
        this.commitTimestamp = commitTimestamp;
        this.changedKeys = Sets.newHashSet(changedKeys);
    }


    public TransformCommitOperation(String branch, long commitTimestamp, Set<QualifiedKey> changedKeys) {
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), commitTimestamp, changedKeys);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        return this.commitTimestamp;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return timestamp >= this.commitTimestamp;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public Set<QualifiedKey> getChangedKeys() {
        return Collections.unmodifiableSet(changedKeys);
    }

    @Override
    public String toString() {
        return "TransformCommit[target: " + this.getBranch() + "@" + this.getCommitTimestamp() + ", wallClockTime: " + this.getWallClockTime() + ", changedKeys: " + this.changedKeys.size() + "]";
    }
}
