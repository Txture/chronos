package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.*;
import org.chronos.chronodb.inmemory.builder.ChronoDBInMemoryBuilder;
import org.chronos.chronodb.inmemory.builder.ChronoDBInMemoryBuilderImpl;
import org.chronos.chronodb.internal.api.BranchManagerInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.DatebackManagerInternal;
import org.chronos.chronodb.internal.api.StatisticsManagerInternal;
import org.chronos.chronodb.internal.api.cache.ChronoDBCache;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.impl.engines.base.AbstractChronoDB;
import org.chronos.chronodb.internal.impl.index.DocumentBasedIndexManager;
import org.chronos.chronodb.internal.impl.query.StandardQueryManager;
import org.chronos.common.version.ChronosVersion;

import static com.google.common.base.Preconditions.*;

public class InMemoryChronoDB extends AbstractChronoDB {

    // =================================================================================================================
    // CONSTANTS
    // =================================================================================================================

    public static final String BACKEND_NAME = "inmemory";

    public static final Class<? extends ChronoDBInMemoryBuilder> BUILDER = ChronoDBInMemoryBuilderImpl.class;

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final InMemoryBranchManager branchManager;
    private final InMemorySerializationManager serializationManager;
    private final IndexManager indexManager;
    private final StandardQueryManager queryManager;
    private final InMemoryMaintenanceManager maintenanceManager;
    private final DatebackManagerInternal datebackManager;
    private final InMemoryStatisticsManager statisticsManager;
    private final BackupManager backupManager;

    private final ChronoDBCache cache;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public InMemoryChronoDB(final ChronoDBConfiguration configuration) {
        super(configuration);
        this.branchManager = new InMemoryBranchManager(this);
        this.serializationManager = new InMemorySerializationManager();
        this.queryManager = new StandardQueryManager(this);
        this.indexManager = new DocumentBasedIndexManager(this, new InMemoryIndexManagerBackend(this));
        this.maintenanceManager = new InMemoryMaintenanceManager(this);
        this.datebackManager = new InMemoryDatebackManager(this);
        this.statisticsManager = new InMemoryStatisticsManager(this);
        this.backupManager = new InMemoryBackupManager(this);
        this.cache = ChronoDBCache.createCacheForConfiguration(configuration);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public BranchManagerInternal getBranchManager() {
        return this.branchManager;
    }

    @Override
    public SerializationManager getSerializationManager() {
        return this.serializationManager;
    }

    @Override
    public QueryManager getQueryManager() {
        return this.queryManager;
    }

    @Override
    public IndexManager getIndexManager() {
        return this.indexManager;
    }

    @Override
    public MaintenanceManager getMaintenanceManager() {
        return this.maintenanceManager;
    }

    @Override
    public StatisticsManagerInternal getStatisticsManager(){
        return this.statisticsManager;
    }

    @Override
    public DatebackManagerInternal getDatebackManager() {
        return this.datebackManager;
    }

    @Override
    public BackupManager getBackupManager() {
        return backupManager;
    }

    @Override
    public ChronoDBCache getCache() {
        return this.cache;
    }

    @Override
    public boolean isFileBased() {
        return false;
    }

    @Override
    public void updateChronosVersionTo(final ChronosVersion chronosVersion) {
        checkNotNull(chronosVersion, "Precondition violation - argument 'chronosVersion' must not be NULL!");
        // we can safely ignore this; in-memory DBs never need to be migrated.
    }

    @Override
    public ChronosVersion getStoredChronosVersion() {
        // well, we don't have any other possibility here, really...
        return ChronosVersion.getCurrentVersion();
    }

    @Override
    public ChronoDBFeatures getFeatures() {
        return new InMemoryFeatures();
    }

    // =====================================================================================================================
    // INTERNAL METHODS
    // =====================================================================================================================


    @Override
    public boolean requiresAutoReindexAfterDumpRead() {
        return true;
    }

    @Override
    protected void updateBuildVersionInDatabase() {
        // this isn't applicable for in-memory databases because the chronos build version
        // can never change during a live JVM session.
    }

}
