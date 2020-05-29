package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.google.common.collect.Sets;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.dateback.log.AbstractDatebackOperation;
import org.chronos.chronodb.internal.impl.dateback.log.v2.TransformCommitOperation2;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

/**
 * This class has been deprecated in favour of the newer version {@link TransformCommitOperationLog2}.
 *
 * <p>
 * We still keep it here for backwards compatibility / deserialization purposes.
 * </p>
 */
@Deprecated
@XStreamAlias("TransformCommitOperation")
public class TransformCommitOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long commitTimestamp;
    private Set<QualifiedKey> changedKeys;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected TransformCommitOperationLog(){
        // default constructor for (de-)serialization
    }

    public TransformCommitOperationLog(String id, String branch, long wallClockTime, long commitTimestamp, Set<QualifiedKey> changedKeys) {
        super(id, branch, wallClockTime);
        checkArgument(commitTimestamp >= 0, "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(changedKeys, "Precondition violation - argument 'changedKeys' must not be NULL!");
        this.commitTimestamp = commitTimestamp;
        this.changedKeys = Sets.newHashSet(changedKeys);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

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
