package org.chronos.chronodb.internal.impl.engines.base;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.CommitMetadataFilter;
import org.chronos.chronodb.api.builder.transaction.ChronoDBTransactionBuilder;
import org.chronos.chronodb.api.exceptions.ChronosBuildVersionConflictException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronodb.internal.api.*;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.DefaultTransactionConfiguration;
import org.chronos.chronodb.internal.impl.builder.transaction.DefaultTransactionBuilder;
import org.chronos.chronodb.internal.impl.dump.CommitMetadataMap;
import org.chronos.chronodb.internal.util.ThreadBound;
import org.chronos.common.autolock.AutoLock;
import org.chronos.common.version.ChronosVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractChronoDB implements ChronoDB, ChronoDBInternal {

    private static final Logger log = LoggerFactory.getLogger(AbstractChronoDB.class);

    protected final ReadWriteLock dbLock;

    private final ChronoDBConfiguration configuration;
    private final Set<ChronoDBShutdownHook> shutdownHooks;
    private final CommitMetadataFilter commitMetadataFilter;
    private CommitTimestampProvider commitTimestampProvider;

    private final ThreadBound<AutoLock> exclusiveLockHolder;
    private final ThreadBound<AutoLock> nonExclusiveLockHolder;

    private boolean closed = false;

    protected AbstractChronoDB(final ChronoDBConfiguration configuration) {
        checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
        this.configuration = configuration;
        this.dbLock = new ReentrantReadWriteLock(false);
        this.exclusiveLockHolder = ThreadBound.createWeakReference();
        this.nonExclusiveLockHolder = ThreadBound.createWeakReference();
        this.shutdownHooks = Collections.synchronizedSet(Sets.newHashSet());
        this.commitMetadataFilter = this.createMetadataFilterFromConfiguration(configuration);
    }


    // =================================================================================================================
    // POST CONSTRUCT
    // =================================================================================================================

    @Override
    public void postConstruct() {
        // check if any branch needs recovery (we do this even in read-only mode, as the DB
        // might not be readable otherwise)
        for (Branch branch : this.getBranchManager().getBranches()) {
            TemporalKeyValueStore tkvs = ((BranchInternal) branch).getTemporalKeyValueStore();
            tkvs.performStartupRecoveryIfRequired();
        }
        this.commitTimestampProvider = new CommitTimestampProviderImpl(this.getBranchManager().getMaxNowAcrossAllBranches());
    }

    // =================================================================================================================
    // SHUTDOWN HANDLING
    // =================================================================================================================

    public void addShutdownHook(final ChronoDBShutdownHook hook) {
        checkNotNull(hook, "Precondition violation - argument 'hook' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            this.shutdownHooks.add(hook);
        }
    }

    public void removeShutdownHook(final ChronoDBShutdownHook hook) {
        checkNotNull(hook, "Precondition violation - argument 'hook' must not be NULL!");
        try (AutoLock lock = this.lockNonExclusive()) {
            this.shutdownHooks.remove(hook);
        }
    }

    @Override
    public final void close() {
        if (this.isClosed()) {
            return;
        }
        try (AutoLock lock = this.lockExclusive()) {
            for (ChronoDBShutdownHook hook : this.shutdownHooks) {
                hook.onShutdown();
            }
            this.closed = true;
        }
    }

    @Override
    public final boolean isClosed() {
        return this.closed;
    }

    // =================================================================================================================
    // TRANSACTION HANDLING
    // =================================================================================================================

    @Override
    public ChronoDBTransactionBuilder txBuilder() {
        return new DefaultTransactionBuilder(this);
    }

    @Override
    public ChronoDBTransaction tx(final TransactionConfigurationInternal configuration) {
        checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
        if (this.getDatebackManager().isDatebackRunning() && !configuration.isAllowedDuringDateback()) {
            throw new IllegalStateException(
                "Unable to open transaction: this database is currently executing a Dateback process!");
        }
        try (AutoLock lock = this.lockNonExclusive()) {
            String branchName = configuration.getBranch();
            if (this.getBranchManager().existsBranch(branchName) == false) {
                throw new InvalidTransactionBranchException(
                    "There is no branch '" + branchName + "' in this ChronoDB!");
            }
            TemporalKeyValueStore tkvs = this.getTKVS(branchName);
            long now = tkvs.getNow();
            if (configuration.isTimestampNow() == false && configuration.getTimestamp() > now) {
                if (log.isDebugEnabled()) {
                    log.debug("Invalid timestamp. Requested = " + configuration.getTimestamp() + ", now = "
                        + now + ", branch = '" + branchName + "'");
                }
                throw new InvalidTransactionTimestampException(
                    "Cannot open transaction at the given date or timestamp: it's after the latest commit! Latest commit: "
                        + now + ", transaction timestamp: " + configuration.getTimestamp() + ", branch: " + branchName);
            }
            TransactionConfigurationInternal txConfig = configuration;
            if (this.getConfiguration().isReadOnly()) {
                // set the read-only flag on every transaction
                txConfig = new DefaultTransactionConfiguration(configuration, c -> c.setReadOnly(true));
            }
            return tkvs.tx(txConfig);
        }
    }

    // =================================================================================================================
    // DUMP CREATION & DUMP LOADING
    // =================================================================================================================

    @Override
    public CloseableIterator<ChronoDBEntry> entryStream() {
        return this.entryStream(0L, System.currentTimeMillis());
    }

    @Override
    public CloseableIterator<ChronoDBEntry> entryStream(long minTimestamp, long maxTimestamp) {
        Set<String> branchNames = this.getBranchManager().getBranchNames();
        Iterator<String> branchIterator = branchNames.iterator();
        Iterator<CloseableIterator<ChronoDBEntry>> branchStreams = Iterators.transform(branchIterator,
            (final String branch) -> entryStream(branch, minTimestamp, maxTimestamp)
        );
        return CloseableIterator.concat(branchStreams);
    }

    @Override
    public CloseableIterator<ChronoDBEntry> entryStream(String branch, long minTimestamp, long maxTimestamp) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        if (!this.getBranchManager().existsBranch(branch)) {
            throw new IllegalArgumentException("There is no branch named '" + branch + "'!");
        }
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(minTimestamp <= maxTimestamp, "Precondition violation - argument 'minTimestamp' must be less than or equal to 'maxTimestamp'!");
        return this.getTKVS(branch).allEntriesIterator(minTimestamp, maxTimestamp);
    }

    @Override
    public void loadEntries(final List<ChronoDBEntry> entries) {
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        this.loadEntries(entries, false);
    }

    protected void loadEntries(final List<ChronoDBEntry> entries, final boolean force) {
        SetMultimap<String, ChronoDBEntry> branchToEntries = HashMultimap.create();
        for (ChronoDBEntry entry : entries) {
            String branchName = entry.getIdentifier().getBranchName();
            branchToEntries.put(branchName, entry);
        }
        // insert into the branches
        for (String branchName : branchToEntries.keySet()) {
            Set<ChronoDBEntry> branchEntries = branchToEntries.get(branchName);
            BranchInternal branch = this.getBranchManager().getBranch(branchName);
            branch.getTemporalKeyValueStore().insertEntries(branchEntries, force);
        }
    }

    @Override
    public void loadCommitTimestamps(final CommitMetadataMap commitMetadata) {
        checkNotNull(commitMetadata, "Precondition violation - argument 'commitMetadata' must not be NULL!");
        for (String branchName : commitMetadata.getContainedBranches()) {
            SortedMap<Long, Object> branchCommitMetadata = commitMetadata.getCommitMetadataForBranch(branchName);
            BranchInternal branch = this.getBranchManager().getBranch(branchName);
            CommitMetadataStore metadataStore = branch.getTemporalKeyValueStore().getCommitMetadataStore();
            for (Entry<Long, Object> entry : branchCommitMetadata.entrySet()) {
                metadataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    // =================================================================================================================
    // LOCKING
    // =================================================================================================================

    @Override
    public AutoLock lockExclusive() {
        AutoLock lockHolder = this.exclusiveLockHolder.get();
        if (lockHolder == null) {
            lockHolder = AutoLock.createBasicLockHolderFor(this.dbLock.writeLock());
            this.exclusiveLockHolder.set(lockHolder);
        }
        // lockHolder.releaseLock() is called on lockHolder.close()
        lockHolder.acquireLock();
        return lockHolder;
    }

    @Override
    public AutoLock lockNonExclusive() {
        AutoLock lockHolder = this.nonExclusiveLockHolder.get();
        if (lockHolder == null) {
            lockHolder = AutoLock.createBasicLockHolderFor(this.dbLock.readLock());
            this.nonExclusiveLockHolder.set(lockHolder);
        }
        // lockHolder.releaseLock() is called on lockHolder.close()
        lockHolder.acquireLock();
        return lockHolder;
    }

    // =================================================================================================================
    // MISCELLANEOUS
    // =================================================================================================================

    @Override
    public ChronoDBConfiguration getConfiguration() {
        return this.configuration;
    }

    protected TemporalKeyValueStore getTKVS(final String branchName) {
        BranchInternal branch = this.getBranchManager().getBranch(branchName);
        return branch.getTemporalKeyValueStore();
    }

    @Override
    public CommitMetadataFilter getCommitMetadataFilter() {
        return this.commitMetadataFilter;
    }

    protected CommitMetadataFilter createMetadataFilterFromConfiguration(final ChronoDBConfiguration configuration) {
        Class<? extends CommitMetadataFilter> filterClass = configuration.getCommitMetadataFilterClass();
        if (filterClass == null) {
            // no filter specified
            return null;
        }
        try {
            Constructor<? extends CommitMetadataFilter> constructor = filterClass.getConstructor();
            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }
            return constructor.newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            log.warn("Configuration warning: The given Commit Metadata Filter class '" + filterClass.getName() + "' could not be instantiated (" + e.getClass().getSimpleName() + ")! Does it have a default constructor? No filter will be used.");
            return null;
        }
    }

    @Override
    public ChronosVersion getCurrentChronosVersion() {
        return ChronosVersion.getCurrentVersion();
    }

    @Override
    public CommitTimestampProvider getCommitTimestampProvider() {
        return commitTimestampProvider;
    }

    // =====================================================================================================================
    // ABSTRACT METHOD DECLARATIONS
    // =====================================================================================================================

    /**
     * Updates the Chronos Build Version in the database.
     *
     * <p>
     * Implementations of this method should follow this algorithm:
     * <ol>
     * <li>Check if the version identifier in the database exists.
     * <ol>
     * <li>If there is no version identifier, write {@link ChronosVersion#getCurrentVersion()}.
     * <li>If there is a version identifier, and it is smaller than {@link ChronosVersion#getCurrentVersion()},
     * overwrite it (<i>Note:</i> in the future, we may perform data migration steps here).
     * <li>If there is a version identifier larger than {@link ChronosVersion#getCurrentVersion()}, throw a
     * {@link ChronosBuildVersionConflictException}.
     * </ol>
     * </ol>
     *
     * @param readOnly Set to <code>true</code> if the DB is in read-only mode. If read-only mode is enabled,
     *                 no changes must be performed to the persistent data. If the current software version is
     *                 incompatible with the persistent format in any way (e.g. a migration would be necessary),
     *                 an exception should be thrown if read-only is enabled.
     *
     * @return The current version of chronos which was originally stored in the database
     */
    protected abstract ChronosVersion updateBuildVersionInDatabase(boolean readOnly);

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    public interface ChronoDBShutdownHook {

        public void onShutdown();

    }

}
