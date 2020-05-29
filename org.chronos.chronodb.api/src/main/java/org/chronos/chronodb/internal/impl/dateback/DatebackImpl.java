package org.chronos.chronodb.internal.impl.dateback;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.MutableTransactionConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.api.dateback.log.DatebackLogger;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;
import org.chronos.chronodb.internal.impl.dateback.log.*;
import org.chronos.chronodb.internal.impl.dateback.log.v2.TransformCommitOperation2;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class DatebackImpl implements Dateback, AutoCloseable {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoDBInternal db;
    private final String branch;
    private boolean closed;

    private final DatebackLogger logger;

    private long earliestTouchedTimestamp = Long.MAX_VALUE;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public DatebackImpl(final ChronoDBInternal dbInstance, final String branch, final DatebackLogger logger) {
        checkNotNull(dbInstance, "Precondition violation - argument 'dbInstance' must not be NULL!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkNotNull(logger, "Precondition violation - argument 'logger' must not be NULL!");
        this.db = dbInstance;
        this.logger = logger;
        this.branch = branch;
        this.closed = false;
    }

    // =================================================================================================================
    // LIFECYCLE
    // =================================================================================================================

    @Override
    public void close() {
        if (this.closed) {
            return;
        }
        this.cleanup();
        this.closed = true;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public boolean purgeEntry(final String keyspace, final String key, final long timestamp) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be neagtive!");
        this.assertNotClosed();
        boolean purged = this.getTKVS().datebackPurgeEntry(keyspace, key, timestamp);
        if (purged) {
            this.updateEarliestTouchedTimestamp(timestamp);
            this.logger.logDatebackOperation(new PurgeEntryOperation(this.branch, timestamp, keyspace, key));
        }
        return purged;
    }


    @Override
    public void purgeKey(final String keyspace, final String key) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeKey(keyspace, key);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        long minTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).min().orElseThrow(IllegalStateException::new);
        long maxTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).max().orElseThrow(IllegalStateException::new);
        this.logger.logDatebackOperation(new PurgeKeyOperation(this.branch, keyspace, key, minTimestamp, maxTimestamp));
    }

    @Override
    public void purgeKey(final String keyspace, final String key, final BiPredicate<Long, Object> predicate) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(predicate, "Precondition violation - argument 'predicate' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeKey(keyspace, key, predicate);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        long minTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).min().orElseThrow(IllegalStateException::new);
        long maxTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).max().orElseThrow(IllegalStateException::new);
        this.logger.logDatebackOperation(new PurgeKeyOperation(this.branch, keyspace, key, minTimestamp, maxTimestamp));
    }

    @Override
    public void purgeKey(final String keyspace, final String key, final long purgeRangeStart,
                         final long purgeRangeEnd) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeKey(keyspace, key, purgeRangeStart,
            purgeRangeEnd);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        long minTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).min().orElseThrow(IllegalStateException::new);
        long maxTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).max().orElseThrow(IllegalStateException::new);
        this.logger.logDatebackOperation(new PurgeKeyOperation(this.branch, keyspace, key, minTimestamp, maxTimestamp));
    }

    @Override
    public void purgeKeyspace(final String keyspace, final long purgeRangeStart, final long purgeRangeEnd) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeKeyspace(keyspace, purgeRangeStart, purgeRangeEnd);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        long minTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).min().orElseThrow(IllegalStateException::new);
        long maxTimestamp = changed.stream().mapToLong(TemporalKey::getTimestamp).max().orElseThrow(IllegalStateException::new);
        this.logger.logDatebackOperation(new PurgeKeyspaceOperation(this.branch, keyspace, minTimestamp, maxTimestamp));
    }

    @Override
    public void purgeCommit(final long commitTimestamp) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeCommit(commitTimestamp);
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(new PurgeCommitsOperation(this.branch, Collections.singleton(commitTimestamp)));
    }

    @Override
    public void purgeCommits(final long purgeRangeStart, final long purgeRangeEnd) {
        checkArgument(purgeRangeStart >= 0,
            "Precondition violation - argument 'purgeRangeStart' must not be negative!");
        checkArgument(purgeRangeEnd >= purgeRangeStart,
            "Precondition violation - argument 'purgeRangeEnd' must be greater than or equal to 'purgeRangeStart'!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackPurgeCommits(purgeRangeStart, purgeRangeEnd);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        Set<Long> timestamps = changed.stream().map(TemporalKey::getTimestamp).collect(Collectors.toSet());
        logger.logDatebackOperation(new PurgeCommitsOperation(this.branch, timestamps));
    }

    @Override
    public void inject(final String keyspace, final String key, final long timestamp, final Object value) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(keyspace, key, timestamp, value);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, Sets.newHashSet(QualifiedKey.create(keyspace, key)), false)
        );
    }

    @Override
    public void inject(final String keyspace, final String key, final long timestamp, final Object value,
                       final Object commitMetadata) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(keyspace, key, timestamp, value,
            commitMetadata);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, Sets.newHashSet(QualifiedKey.create(keyspace, key)), commitMetadata != null)
        );
    }

    @Override
    public void inject(final String keyspace, final String key, final long timestamp, final Object value,
                       final Object commitMetadata, final boolean overrideCommitMetadata) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(keyspace, key, timestamp, value, commitMetadata,
            overrideCommitMetadata);
        if(changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, Sets.newHashSet(QualifiedKey.create(keyspace, key)), overrideCommitMetadata || commitMetadata != null)
        );
    }

    @Override
    public void inject(final long timestamp, final Map<QualifiedKey, Object> entries) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(timestamp, entries);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, changed.stream().map(TemporalKey::toQualifiedKey).collect(Collectors.toSet()), false)
        );
    }

    @Override
    public void inject(final long timestamp, final Map<QualifiedKey, Object> entries, final Object commitMetadata) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(timestamp, entries, commitMetadata);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, changed.stream().map(TemporalKey::toQualifiedKey).collect(Collectors.toSet()), commitMetadata != null)
        );
    }

    @Override
    public void inject(final long timestamp, final Map<QualifiedKey, Object> entries, final Object commitMetadata,
                       final boolean overrideCommitMetadata) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackInject(timestamp, entries, commitMetadata,
            overrideCommitMetadata);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new InjectEntriesOperation(this.branch, timestamp, changed.stream().map(TemporalKey::toQualifiedKey).collect(Collectors.toSet()), overrideCommitMetadata || commitMetadata != null)
        );
    }

    @Override
    public void transformEntry(final String keyspace, final String key, final long timestamp,
                               final Function<Object, Object> transformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackTransformEntry(keyspace, key, timestamp,
            transformation);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(new TransformValuesOperation(this.branch, keyspace, key, Sets.newHashSet(timestamp)));
    }

    @Override
    public void transformValuesOfKey(final String keyspace, final String key,
                                     final BiFunction<Long, Object, Object> transformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackTransformValuesOfKey(keyspace, key, transformation);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new TransformValuesOperation(this.branch, keyspace, key, changed.stream().map(TemporalKey::getTimestamp).collect(Collectors.toSet()))
        );
    }

    @Override
    public void transformCommit(final long commitTimestamp,
                                final Function<Map<QualifiedKey, Object>, Map<QualifiedKey, Object>> transformation) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(transformation, "Precondition violation - argument 'transformation' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackTransformCommit(commitTimestamp, transformation);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new TransformCommitOperation2(this.branch, commitTimestamp)
        );
    }

    @Override
    public void transformValuesOfKeyspace(final String keyspace, final KeyspaceValueTransformation valueTransformation) {
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(valueTransformation, "Precondition violation - argument 'valueTransformation' must not be NULL!");
        this.assertNotClosed();
        Collection<TemporalKey> changed = this.getTKVS().datebackTransformValuesOfKeyspace(keyspace, valueTransformation);
        if(changed == null || changed.isEmpty()){
            return;
        }
        this.updateEarliestTouchedTimestamp(changed);
        this.logger.logDatebackOperation(
            new TransformValuesOfKeyspaceOperation(this.branch, keyspace, changed.stream().mapToLong(TemporalKey::getTimestamp).min().orElse(Long.MAX_VALUE))
        );
    }

    @Override
    public void updateCommitMetadata(final long commitTimestamp, final Object newMetadata) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        this.assertNotClosed();
        this.getTKVS().datebackUpdateCommitMetadata(commitTimestamp, newMetadata);
        this.logger.logDatebackOperation(new UpdateCommitMetadataOperation(this.branch, commitTimestamp));
    }

    @Override
    public void cleanup() {
        this.assertNotClosed();
        if(this.earliestTouchedTimestamp == Long.MAX_VALUE){
            // nothing touched
            return;
        }
        this.getTKVS().datebackCleanup(this.branch, this.earliestTouchedTimestamp);
        this.earliestTouchedTimestamp = Long.MAX_VALUE;
    }

    @Override
    public long getHighestUntouchedTimestamp() {
        // the highest untouched timestamp is always one lower than the lowest one we touched
        return this.earliestTouchedTimestamp - 1;
    }

    @Override
    public Object get(final long timestamp, final String keyspace, final String key) {
        this.assertTimestampWithinValidRange(timestamp);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        return datebackSafeTx(timestamp).get(keyspace, key);
    }

    @Override
    public Set<String> keySet(final long timestamp, final String keyspace) {
        this.assertTimestampWithinValidRange(timestamp);
        checkNotNull(keyspace, "Precondition violation - argument 'keyspace' must not be NULL!");
        return this.datebackSafeTx(timestamp).keySet(keyspace);
    }

    @Override
    public Set<String> keyspaces(final long timestamp) {
        this.assertTimestampWithinValidRange(timestamp);
        return this.datebackSafeTx(timestamp).keyspaces();
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private void assertTimestampWithinValidRange(long timestamp) {
        if (timestamp > this.getHighestUntouchedTimestamp()) {
            throw new IllegalArgumentException("Cannot query timestamp " + timestamp + ", as it has been influenced by a dateback operation already. The highest untouched timestamp is " + this.getHighestUntouchedTimestamp() + ".");
        }
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must  not be negative!");
    }

    private ChronoDBTransaction datebackSafeTx(long timestamp) {
        this.assertTimestampWithinValidRange(timestamp);
        MutableTransactionConfiguration txConfig = new DefaultTransactionConfiguration();
        txConfig.setBranch(this.branch);
        txConfig.setTimestamp(timestamp);
        txConfig.setReadOnly(true);
        txConfig.setAllowedDuringDateback(true);
        return this.db.tx(txConfig);
    }


    protected void assertNotClosed() {
        if (this.closed) {
            throw new IllegalStateException("Dateback process has already been terminated!");
        }
    }

    protected TemporalKeyValueStore getTKVS() {
        BranchInternal branch = this.db.getBranchManager().getBranch(this.branch);
        TemporalKeyValueStore store = branch.getTemporalKeyValueStore();
        return store;
    }


    protected void updateEarliestTouchedTimestamp(final long timestamp) {
        if(timestamp < this.earliestTouchedTimestamp){
            this.earliestTouchedTimestamp = timestamp;
        }
    }

    protected void updateEarliestTouchedTimestamp(final Collection<TemporalKey> changSet) {
        for(TemporalKey key : changSet){
            this.updateEarliestTouchedTimestamp(key.getTimestamp());
        }
    }

}