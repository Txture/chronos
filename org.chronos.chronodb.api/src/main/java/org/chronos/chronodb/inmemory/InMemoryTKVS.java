package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.internal.api.*;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.WriteAheadLogToken;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.*;

public class InMemoryTKVS extends AbstractTemporalKeyValueStore implements TemporalKeyValueStore {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final AtomicLong now = new AtomicLong(0);
    private final CommitMetadataStore commitMetadataStore;

    private WriteAheadLogToken walToken = null;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public InMemoryTKVS(final ChronoDBInternal db, final BranchInternal branch) {
        super(db, branch);
        this.commitMetadataStore = new InMemoryCommitMetadataStore(db, branch);
        TemporalDataMatrix matrix = this.createMatrix(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, 0L);
        this.keyspaceToMatrix.put(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, matrix);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    protected long getNowInternal() {
        return this.now.get();
    }

    @Override
    protected void setNow(final long timestamp) {
        this.now.set(timestamp);
    }

    @Override
    protected TemporalDataMatrix createMatrix(final String keyspace, final long timestamp) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return new TemporalInMemoryMatrix(keyspace, timestamp);
    }

    @Override
    protected synchronized void performWriteAheadLog(final WriteAheadLogToken token) {
        checkNotNull(token, "Precondition violation - argument 'token' must not be NULL!");
        this.walToken = token;
    }

    @Override
    protected synchronized void clearWriteAheadLogToken() {
        this.walToken = null;
    }

    @Override
    public void performStartupRecoveryIfRequired() {
        // startup recovery is never needed for in-memory elements
    }

    @Override
    protected synchronized WriteAheadLogToken getWriteAheadLogTokenIfExists() {
        return this.walToken;
    }

    @Override
    public CommitMetadataStore getCommitMetadataStore() {
        return this.commitMetadataStore;
    }

}
