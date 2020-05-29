package org.chronos.chronodb.internal.impl.dateback.log;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.dateback.log.IInjectEntriesOperation;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public class InjectEntriesOperation extends AbstractDatebackOperation implements IInjectEntriesOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long operationTimestamp;
    private boolean commitMetadataOverride;
    private Set<QualifiedKey> injectedKeys = Collections.emptySet();

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected InjectEntriesOperation(){
        // default constructor for (de-)serialization
    }

    public InjectEntriesOperation(String id, String branch, long wallClockTime, long timestamp, Set<QualifiedKey> injectedKeys, boolean commitMetadataOverride){
        super(id, branch, wallClockTime);
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(injectedKeys, "Precondition violation - argument 'injectedKeys' must not be NULL!");
        this.operationTimestamp = timestamp;
        this.commitMetadataOverride = commitMetadataOverride;
        this.injectedKeys = Sets.newHashSet(injectedKeys);
    }

    public InjectEntriesOperation(String branch, long timestamp, Set<QualifiedKey> injectedKeys, boolean commitMetadataOverride){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis(), timestamp, injectedKeys, commitMetadataOverride);
    }

    // =================================================================================================================
    // PUBCIC API
    // =================================================================================================================


    @Override
    public long getEarliestAffectedTimestamp() {
        return this.operationTimestamp;
    }

    @Override
    public boolean affectsTimestamp(final long timestamp) {
        return this.operationTimestamp <= timestamp;
    }

    @Override
    public long getOperationTimestamp() {
        return this.operationTimestamp;
    }

    @Override
    public boolean isCommitMetadataOverride() {
        return commitMetadataOverride;
    }

    public Set<QualifiedKey> getInjectedKeys() {
        return Collections.unmodifiableSet(injectedKeys);
    }

    @Override
    public String toString() {
        return "InjectEntries[target: " + this.getBranch() + "@" + this.getOperationTimestamp() + ", wallClockTime: " + this.getWallClockTime() + ", injectedKeys: " + this.getInjectedKeys().size() + "]";
    }
}
