package org.chronos.chronodb.exodus.configuration

import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.chronodb.internal.impl.ChronoDBBaseConfiguration
import org.chronos.common.configuration.ParameterValueConverters
import org.chronos.common.configuration.annotation.Parameter
import org.chronos.common.configuration.annotation.ValueConverter
import java.io.File


class ExodusChronoDBConfiguration : ChronoDBBaseConfiguration() {


    companion object {

        // =================================================================================================================
        // CONSTANTS
        // =================================================================================================================

        private const val EXODUS_PREFIX = ChronoDBConfiguration.NS_DOT + "exodus."

        /**
         * The working file where the data is stored.
         *
         * Sibling files and folders may also be created for secondary indexing.
         *
         * Type: string
         * Values: any valid filepath in your file system
         * Default value: none (mandatory setting)
         * Maps to: [.workDirectory]
         */
        const val WORK_DIR = ChronoDBConfiguration.NS_DOT + "storage.work_directory"

        /**
         * Decides how many Exodus environments to keep open at any given point in time.
         *
         * If you have a large database with many chunks, consider increasing this value to improve
         * performance (as fewer chunk open/close operations need to be executed). However, do note
         * that a higher setting increases RAM usage and also requires more file handles.
         *
         * Type: integer
         * Values: any positive integer >= 1
         * Default value: 20
         * Maps to: [.keepOpenEnvironments]
         */
        const val KEEP_OPEN_ENVIRONMENTS = ChronoDBConfiguration.NS_DOT + "storage.keep_open_environments"

        /**
         * Decides how often the background task which closes excess open environments is executed.
         *
         * By default, this task is executed once per minute. Doing it more often will result in more
         * eagerly releasing file handles, but will cause additional load on the system.
         *
         * Type: integer
         * Values: any positive integer >= 1
         * Default value: 60
         * Maps to: [.environmentCleanPeriodSeconds]
         */
        const val ENVIRONMENT_CLEAN_PERIOD_SECONDS = ChronoDBConfiguration.NS_DOT + "storage.environment_clean_period_seconds"

        /**
         * Determines the batch size (in number of entries) for the rollover process.
         *
         * Larger batches will likely execute faster, however the RAM consumption will also be higher.
         *
         * Type: integer
         * Values: any positive integer >= 1
         * Default value: 50.000
         * Maps to: [.rolloverBatchSize]
         */
        const val ROLLOVER_BATCH_SIZE = ChronoDBConfiguration.NS_DOT + "storage.rollover.batch_size"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.MEMORY_USAGE]
         */
        const val EXODUS_MEMORY_USAGE = EXODUS_PREFIX + "memoryUsage"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.MEMORY_USAGE_PERCENTAGE]
         */
        const val EXODUS_MEMORY_USAGE_PERCENTAGE = EXODUS_PREFIX + "memoryUsagePercentage"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.CIPHER_ID]
         */
        const val EXODUS_CIPHER_ID =EXODUS_PREFIX + "cipherId"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.CIPHER_KEY]
         */
        const val EXODUS_CIPHER_KEY =EXODUS_PREFIX + "cipherKey"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.CIPHER_BASIC_IV]
         */
        const val EXODUS_CIPHER_BASIC_IV =EXODUS_PREFIX + "cipherBasicIV"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_DURABLE_WRITE]
         */
        const val EXODUS_LOG_DURABLE_WRITE =EXODUS_PREFIX + "log.durableWrite"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_FILE_SIZE]
         */
        const val EXODUS_LOG_FILE_SIZE =EXODUS_PREFIX + "log.fileSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_LOCK_TIMEOUT]
         */
        const val EXODUS_LOG_LOCK_TIMEOUT =EXODUS_PREFIX + "log.lockTimeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_LOCK_ID]
         */
        const val EXODUS_LOG_LOCK_ID =EXODUS_PREFIX + "log.lockID"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_PAGE_SIZE]
         */
        const val EXODUS_LOG_CACHE_PAGE_SIZE =EXODUS_PREFIX + "log.cache.pageSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_OPEN_FILES]
         */
        const val EXODUS_LOG_CACHE_OPEN_FILES =EXODUS_PREFIX + "log.cache.openFilesCount"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_USE_NIO]
         */
        const val EXODUS_LOG_CACHE_USE_NIO =EXODUS_PREFIX + "log.cache.useNIO"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD]
         */
        const val EXODUS_LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD =EXODUS_PREFIX + "log.cache.freePhysicalMemoryThreshold"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_SHARED]
         */
        const val EXODUS_LOG_CACHE_SHARED =EXODUS_PREFIX + "log.cache.shared"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_NON_BLOCKING]
         */
        const val EXODUS_LOG_CACHE_NON_BLOCKING =EXODUS_PREFIX + "log.cache.nonBlocking"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_GENERATION_COUNT]
         */
        const val EXODUS_LOG_CACHE_GENERATION_COUNT =EXODUS_PREFIX + "log.cache.generationCount"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CACHE_READ_AHEAD_MULTIPLE]
         */
        const val EXODUS_LOG_CACHE_READ_AHEAD_MULTIPLE =EXODUS_PREFIX + "log.cache.readAheadMultiple"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CLEAN_DIRECTORY_EXPECTED]
         */
        const val EXODUS_LOG_CLEAN_DIRECTORY_EXPECTED =EXODUS_PREFIX + "log.cleanDirectoryExpected"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_CLEAR_INVALID]
         */
        const val EXODUS_LOG_CLEAR_INVALID =EXODUS_PREFIX + "log.clearInvalid"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_SYNC_PERIOD]
         */
        const val EXODUS_LOG_SYNC_PERIOD =EXODUS_PREFIX + "log.syncPeriod"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_FULL_FILE_READ_ONLY]
         */
        const val EXODUS_LOG_FULL_FILE_READ_ONLY =EXODUS_PREFIX + "log.fullFileReadonly"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.LOG_DATA_READER_WRITER_PROVIDER]
         */
        const val EXODUS_LOG_DATA_READER_WRITER_PROVIDER =EXODUS_PREFIX + "log.readerWriterProvider"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_IS_READONLY]
         */
        const val EXODUS_ENV_IS_READONLY =EXODUS_PREFIX + "env.isReadonly"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_FAIL_FAST_IN_READONLY]
         */
        const val EXODUS_ENV_FAIL_FAST_IN_READONLY =EXODUS_PREFIX + "env.failFastInReadonly"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_READONLY_EMPTY_STORES]
         */
        const val EXODUS_ENV_READONLY_EMPTY_STORES =EXODUS_PREFIX + "env.readonly.emptyStores"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_STOREGET_CACHE_SIZE]
         */
        const val EXODUS_ENV_STOREGET_CACHE_SIZE =EXODUS_PREFIX + "env.storeGetCacheSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_STOREGET_CACHE_MIN_TREE_SIZE]
         */
        const val EXODUS_ENV_STOREGET_CACHE_MIN_TREE_SIZE =EXODUS_PREFIX + "env.storeGetCache.minTreeSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_STOREGET_CACHE_MAX_VALUE_SIZE]
         */
        const val EXODUS_ENV_STOREGET_CACHE_MAX_VALUE_SIZE =EXODUS_PREFIX + "env.storeGetCache.maxValueSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_CLOSE_FORCEDLY]
         */
        const val EXODUS_ENV_CLOSE_FORCEDLY =EXODUS_PREFIX + "env.closeForcedly"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_TXN_REPLAY_TIMEOUT]
         */
        const val EXODUS_ENV_TXN_REPLAY_TIMEOUT =EXODUS_PREFIX + "env.txn.replayTimeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_TXN_REPLAY_MAX_COUNT]
         */
        const val EXODUS_ENV_TXN_REPLAY_MAX_COUNT =EXODUS_PREFIX + "env.txn.replayMaxCount"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_TXN_DOWNGRADE_AFTER_FLUSH]
         */
        const val EXODUS_ENV_TXN_DOWNGRADE_AFTER_FLUSH =EXODUS_PREFIX + "env.txn.downgradeAfterFlush"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_TXN_SINGLE_THREAD_WRITES]
         */
        const val EXODUS_ENV_TXN_SINGLE_THREAD_WRITES =EXODUS_PREFIX + "env.txn.singleThreadWrites"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_MAX_PARALLEL_TXNS]
         */
        const val EXODUS_ENV_MAX_PARALLEL_TXNS =EXODUS_PREFIX + "env.maxParallelTxns"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_MAX_PARALLEL_READONLY_TXNS]
         */
        const val EXODUS_ENV_MAX_PARALLEL_READONLY_TXNS =EXODUS_PREFIX + "env.maxParallelReadonlyTxns"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_MONITOR_TXNS_TIMEOUT]
         */
        const val EXODUS_ENV_MONITOR_TXNS_TIMEOUT =EXODUS_PREFIX + "env.monitorTxns.timeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT]
         */
        const val EXODUS_ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT =EXODUS_PREFIX + "env.monitorTxns.expirationTimeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_MONITOR_TXNS_CHECK_FREQ]
         */
        const val EXODUS_ENV_MONITOR_TXNS_CHECK_FREQ =EXODUS_PREFIX + "env.monitorTxns.checkFreq"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.ENV_GATHER_STATISTICS]
         */
        const val EXODUS_ENV_GATHER_STATISTICS =EXODUS_PREFIX + "env.gatherStatistics"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.TREE_MAX_PAGE_SIZE]
         */
        const val EXODUS_TREE_MAX_PAGE_SIZE =EXODUS_PREFIX + "tree.maxPageSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.TREE_DUP_MAX_PAGE_SIZE]
         */
        const val EXODUS_TREE_DUP_MAX_PAGE_SIZE =EXODUS_PREFIX + "tree.dupMaxPageSize"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_ENABLED]
         */
        const val EXODUS_GC_ENABLED =EXODUS_PREFIX + "gc.enabled"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_START_IN]
         */
        const val EXODUS_GC_START_IN =EXODUS_PREFIX + "gc.startIn"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_MIN_UTILIZATION]
         */
        const val EXODUS_GC_MIN_UTILIZATION =EXODUS_PREFIX + "gc.minUtilization"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_RENAME_FILES]
         */
        const val EXODUS_GC_RENAME_FILES =EXODUS_PREFIX + "gc.renameFiles"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_MIN_FILE_AGE]
         */
        const val EXODUS_GC_MIN_FILE_AGE =EXODUS_PREFIX + "gc.fileMinAge"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_FILES_INTERVAL]
         */
        const val EXODUS_GC_FILES_INTERVAL =EXODUS_PREFIX + "gc.filesInterval"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_RUN_PERIOD]
         */
        const val EXODUS_GC_RUN_PERIOD =EXODUS_PREFIX + "gc.runPeriod"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_UTILIZATION_FROM_SCRATCH]
         */
        const val EXODUS_GC_UTILIZATION_FROM_SCRATCH =EXODUS_PREFIX + "gc.utilization.fromScratch"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_UTILIZATION_FROM_FILE]
         */
        const val EXODUS_GC_UTILIZATION_FROM_FILE =EXODUS_PREFIX + "gc.utilization.fromFile"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_USE_EXCLUSIVE_TRANSACTION]
         */
        const val EXODUS_GC_USE_EXCLUSIVE_TRANSACTION =EXODUS_PREFIX + "gc.useExclusiveTransaction"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_TRANSACTION_ACQUIRE_TIMEOUT]
         */
        const val EXODUS_GC_TRANSACTION_ACQUIRE_TIMEOUT =EXODUS_PREFIX + "gc.transactionAcquireTimeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_TRANSACTION_TIMEOUT]
         */
        const val EXODUS_GC_TRANSACTION_TIMEOUT =EXODUS_PREFIX + "gc.transactionTimeout"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.GC_FILES_DELETION_DELAY]
         */
        const val EXODUS_GC_FILES_DELETION_DELAY =EXODUS_PREFIX + "gc.filesDeletionDelay"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.MANAGEMENT_ENABLED]
         */
        const val EXODUS_MANAGEMENT_ENABLED =EXODUS_PREFIX + "managementEnabled"

        /**
         * @see [jetbrains.exodus.env.EnvironmentConfig.MANAGEMENT_OPERATIONS_RESTRICTED]
         */
        const val EXODUS_MANAGEMENT_OPERATIONS_RESTRICTED =EXODUS_PREFIX + "management.operationsRestricted"

    }


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    @Parameter(key = WORK_DIR)
    @ValueConverter(ParameterValueConverters.StringToFileConverter::class)
    var workDirectory: File? = null

    @Parameter(key = KEEP_OPEN_ENVIRONMENTS)
    var keepOpenEnvironments: Int = 20

    @Parameter(key = ENVIRONMENT_CLEAN_PERIOD_SECONDS)
    var environmentCleanPeriodSeconds: Int = 60

    @Parameter(key = ROLLOVER_BATCH_SIZE)
    var rolloverBatchSize: Int = 50_000

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_MEMORY_USAGE, optional = true)
    var exodusMemoryUsage: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_MEMORY_USAGE_PERCENTAGE, optional = true)
    var exodusMemoryUsagePercentage: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_CIPHER_ID, optional = true)
    var exodusCipherId: String? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_CIPHER_KEY, optional = true)
    var exodusCipherKey: String? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_CIPHER_BASIC_IV, optional = true)
    var exodusCipherBasicIV: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_DURABLE_WRITE, optional = true)
    var exodusLogDurableWrite: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_FILE_SIZE, optional = true)
    var exodusLogFileSize: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_LOCK_TIMEOUT, optional = true)
    var exodusLogLockTimeout: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_LOCK_ID, optional = true)
    var exodusLogLockId: String? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_PAGE_SIZE, optional = true)
    var exodusLogCachePageSize: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_OPEN_FILES, optional = true)
    var exodusLogCacheOpenFiles: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_USE_NIO, optional = true)
    var exodusLogCacheUseNio: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, optional = true)
    var exodusLogCacheFreePhysicalMemoryThreshold: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_SHARED, optional = true)
    var exodusLogCacheShared: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_NON_BLOCKING, optional = true)
    var exodusLogCacheNonBlocking: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_GENERATION_COUNT, optional = true)
    var exodusLogCacheGenerationCount: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CACHE_READ_AHEAD_MULTIPLE, optional = true)
    var exodusLogCacheReadAheadMultiple: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CLEAN_DIRECTORY_EXPECTED, optional = true)
    var exodusLogCleanDirectoryExpected: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_CLEAR_INVALID, optional = true)
    var exodusLogClearInvalid: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_SYNC_PERIOD, optional = true)
    var exodusLogSyncPeriod: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_FULL_FILE_READ_ONLY, optional = true)
    var exodusLogFullFileReadOnly: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_LOG_DATA_READER_WRITER_PROVIDER, optional = true)
    var exodusLogDataReaderWriterProvider: String? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_IS_READONLY, optional = true)
    var exodusEnvIsReadOnly: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_FAIL_FAST_IN_READONLY, optional = true)
    var exodusEnvFailFastInReadOnly: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_READONLY_EMPTY_STORES, optional = true)
    var exodusEnvReadonlyEmptyStores: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_STOREGET_CACHE_SIZE, optional = true)
    var exodusEnvStoreGetCacheSize: Integer? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_STOREGET_CACHE_MIN_TREE_SIZE, optional = true)
    var exodusEnvStoreGetCacheMinTreeSize: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_STOREGET_CACHE_MAX_VALUE_SIZE, optional = true)
    var exodusEnvStoreGetCacheMaxValueSize: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_CLOSE_FORCEDLY, optional = true)
    var exodusEnvCloseForcedly: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_TXN_REPLAY_TIMEOUT, optional = true)
    var exodusEnvTxnReplayTimeout: Long? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_TXN_REPLAY_MAX_COUNT, optional = true)
    var exodusEnvTxnReplayMaxCount: Integer? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_TXN_DOWNGRADE_AFTER_FLUSH, optional = true)
    var exodusEnvTxnDowngradeAfterFlush: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_TXN_SINGLE_THREAD_WRITES, optional = true)
    var exodusEnvTxnSingleThreadWrites: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_MAX_PARALLEL_TXNS, optional = true)
    var exodusEnvMaxParallelTxns: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_MAX_PARALLEL_READONLY_TXNS, optional = true)
    var exodusEnvMaxParallelReadonlyTxns: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_MONITOR_TXNS_TIMEOUT, optional = true)
    var exodusEnvMonitorTxnsTimeout: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT, optional = true)
    var exodusEnvMonitorTxnsExpirationTimeout: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_MONITOR_TXNS_CHECK_FREQ, optional = true)
    var exodusEnvMonitorTxnsCheckFreq: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_ENV_GATHER_STATISTICS, optional = true)
    var exodusEnvGatherStatistics: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_TREE_MAX_PAGE_SIZE, optional = true)
    var exodusTreeMaxpageSize: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_TREE_DUP_MAX_PAGE_SIZE, optional = true)
    var exodusTreeDupMaxPageSize: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_ENABLED, optional = true)
    var exodusGcEnabled: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_START_IN, optional = true)
    var exodusGcStartIn: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_MIN_UTILIZATION, optional = true)
    var exodusGcMinUtilization: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_RENAME_FILES, optional = true)
    var exodusGcRenameFiles: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_MIN_FILE_AGE, optional = true)
    var exodusGcMinFileAge: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_FILES_INTERVAL, optional = true)
    var exodusGcFilesInterval: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_RUN_PERIOD, optional = true)
    var exodusGcRunPeriod: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_UTILIZATION_FROM_SCRATCH, optional = true)
    var exodusGcUtilizationFromScratch: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_UTILIZATION_FROM_FILE, optional = true)
    var exodusGcUtilizationFromFile: String? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_FILES_DELETION_DELAY, optional = true)
    var exodusGcFilesDeletionDelay: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_USE_EXCLUSIVE_TRANSACTION, optional = true)
    var exodusGcExclusiveTransaction: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_TRANSACTION_ACQUIRE_TIMEOUT, optional = true)
    var exodusGcTransactionAcquireTimeout: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_GC_TRANSACTION_TIMEOUT, optional = true)
    var exodusGcTransactionTimeout: Int? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_MANAGEMENT_ENABLED, optional = true)
    var exodusManagementEnabled: Boolean? = null

    @Suppress("unused") // used in 'extractExodusConfiguration()'.
    @Parameter(key = EXODUS_MANAGEMENT_OPERATIONS_RESTRICTED, optional = true)
    var exodusManagementOperationsRestricted: Boolean? = null

    fun extractExodusConfiguration(): Map<String, Any> {
        val map = mutableMapOf<String, Any>()
        for(optionMetadata in this.metadata){
            if(optionMetadata.key.startsWith(EXODUS_PREFIX)){
                val exodusKey = optionMetadata.key.removePrefix(ChronoDBConfiguration.NS_DOT)
                val value = optionMetadata.getValue(this) ?: continue
                map[exodusKey] = value
            }
        }
        return map
    }
}