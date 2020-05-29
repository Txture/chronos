package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import com.google.common.collect.Sets;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.dateback.log.AbstractDatebackOperation;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

@XStreamAlias("InjectEntriesOperation")
public class InjectEntriesOperationLog extends DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private long operationTimestamp;
    private boolean commitMetadataOverride;
    private Set<QualifiedKey> injectedKeys = Collections.emptySet();

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected InjectEntriesOperationLog(){
        // default constructor for (de-)serialization
    }

    public InjectEntriesOperationLog(String id, String branch, long wallClockTime, long timestamp, Set<QualifiedKey> injectedKeys, boolean commitMetadataOverride){
        super(id, branch, wallClockTime);
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(injectedKeys, "Precondition violation - argument 'injectedKeys' must not be NULL!");
        this.operationTimestamp = timestamp;
        this.commitMetadataOverride = commitMetadataOverride;
        this.injectedKeys = Sets.newHashSet(injectedKeys);
    }

    // =================================================================================================================
    // PUBCIC API
    // =================================================================================================================

    public long getOperationTimestamp() {
        return this.operationTimestamp;
    }

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
