package org.chronos.chronodb.exodus.manager.chunk

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.key.ChronoIdentifier
import org.chronos.chronodb.exodus.environment.EnvironmentManager
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryDoubleIndexStore
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryLongIndexStore
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryStringIndexStore
import org.chronos.chronodb.exodus.transaction.ExodusChunkTransaction
import org.chronos.chronodb.exodus.transaction.ExodusChunkTransactionImpl
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.transaction.ExodusTransactionImpl
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.chronos.chronodb.internal.impl.stream.entry.ChronoDBEntryImpl
import org.chronos.common.serialization.KryoManager
import java.io.File
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class GlobalChunkManager : AutoCloseable {

    // =================================================================================================================
    // STATIC METHODS
    // =================================================================================================================

    companion object {

        private val log = KotlinLogging.logger {}

        fun create(rootDirectory: File, resolveBranchName: (File) -> String?, environmentManager: EnvironmentManager): GlobalChunkManager {
            requireDirectory(rootDirectory, "rootDirectory")
            // if global directory does not exist, create it
            val globalDirectory = File(rootDirectory, ChronoDBDirectoryLayout.GLOBAL_DIRECTORY)
            globalDirectory.createDirectoryIfNotExists()

            // if branches directory does not exist, create it
            val branchesDirectory = File(rootDirectory, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY)
            branchesDirectory.createDirectoryIfNotExists()

            // if master branch does not exist, create it
            val masterBranch = File(branchesDirectory, ChronoDBDirectoryLayout.MASTER_BRANCH_DIRECTORY)
            masterBranch.createDirectoryIfNotExists()

            val branchNameToChunkManager = branchesDirectory.listFiles().asSequence()
                .filter { it.isDirectory }
                .filter { it.name.startsWith(ChronoDBDirectoryLayout.BRANCH_DIRECTORY_PREFIX) }
                .map { branchDir ->
                    val branchName = resolveBranchName(branchDir)
                    if (branchName == null) {
                        null
                    } else {
                        Pair(branchName, BranchChunkManager.create(branchDir, branchName))
                    }
                }
                .filterNotNull()
                .toMap()
            return GlobalChunkManager(rootDirectory, branchNameToChunkManager, environmentManager)
        }

    }

    // =================================================================================================================
    // PROPERTIES
    // =================================================================================================================

    private val rootDirectory: File
    private val globalDirectory: File
    private val branchesDirectory: File

    private var closed: Boolean
    private val branchDirectoryLock: ReadWriteLock
    private val branchNameToBranchChunkManager: MutableMap<String, BranchChunkManager>
    private val environmentManager: EnvironmentManager

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(rootDirectory: File, branchNameToBranchChunkManager: Map<String, BranchChunkManager>, environmentManager: EnvironmentManager) {
        this.rootDirectory = requireDirectory(rootDirectory, "rootDirectory")
        this.globalDirectory = requireDirectory(File(rootDirectory, ChronoDBDirectoryLayout.GLOBAL_DIRECTORY), "globalDirectory")
        this.branchesDirectory = requireDirectory(File(rootDirectory, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY), "branchesDirectory")
        this.branchNameToBranchChunkManager = branchNameToBranchChunkManager.toMutableMap()
        this.environmentManager = environmentManager
        this.closed = false
        this.branchDirectoryLock = ReentrantReadWriteLock(true)
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Synchronized
    override fun close() {
        this.branchDirectoryLock.writeLock().withLock {
            if (this.closed) {
                return
            }
            this.closed = true
            this.environmentManager.close()
        }
    }

    val isClosed: Boolean
        get() {
            synchronized(this) {
                this.branchDirectoryLock.readLock().withLock {
                    return this.closed
                }
            }
        }

    fun dropAllSecondaryIndexFiles(branchMetadata: IBranchMetadata) {
        for (chunk in this.getOrCreateChunkManagerForBranch(branchMetadata).getAllChunks()) {
            val directory = chunk.indexDirectory
            if (!directory.exists()) {
                continue
            }
            this.openReadWriteTransactionOn(directory).use { tx ->
                val storeNames = tx.getAllStoreNames()
                for (storeName in storeNames) {
                    val stringIndexId = SecondaryStringIndexStore.getIndexIdForStoreName(storeName)
                    if (stringIndexId != null) {
                        tx.removeStore(storeName)
                        continue
                    }
                    val longIndexId = SecondaryLongIndexStore.getIndexIdForStoreName(storeName)
                    if (longIndexId != null) {
                        tx.removeStore(storeName)
                        continue
                    }
                    val doubleIndexId = SecondaryDoubleIndexStore.getIndexIdForStoreName(storeName)
                    if (doubleIndexId != null) {
                        tx.removeStore(storeName)
                        continue
                    }
                }
                tx.commit()
            }
        }
    }

    fun dropSecondaryIndexFiles(indices: Set<SecondaryIndex>) {
        for ((branchName, branchIndices) in indices.groupBy { it.branch }) {
            this.dropChunkIndices(branchIndices, branchName)
        }
    }

    private fun dropChunkIndices(indices: Collection<SecondaryIndex>, branch: String) {
        if (indices.isEmpty()) {
            return
        }
        val indexIds = indices.asSequence().map { it.id }.toSet()
        for (chunk in this.getChunkManagerForBranch(branch).getAllChunks()) {
            val directory = chunk.indexDirectory
            if (!directory.exists()) {
                continue
            }
            this.openReadWriteTransactionOn(directory).use { tx ->
                val storeNames = tx.getAllStoreNames()
                for (storeName in storeNames) {
                    val stringIndexId = SecondaryStringIndexStore.getIndexIdForStoreName(storeName)
                    if (stringIndexId in indexIds) {
                        tx.removeStore(storeName)
                        continue
                    }
                    val longIndexId = SecondaryLongIndexStore.getIndexIdForStoreName(storeName)
                    if (longIndexId in indexIds) {
                        tx.removeStore(storeName)
                        continue
                    }
                    val doubleIndexId = SecondaryDoubleIndexStore.getIndexIdForStoreName(storeName)
                    if (doubleIndexId in indexIds) {
                        tx.removeStore(storeName)
                        continue
                    }
                }
                tx.commit()
            }
        }
    }

    fun hasChunkManagerForBranch(branchName: String): Boolean {
        this.branchDirectoryLock.readLock().withLock {
            return this.branchNameToBranchChunkManager.containsKey(branchName)
        }
    }

    fun getChunkManagerForBranch(branchName: String): BranchChunkManager {
        this.branchDirectoryLock.readLock().withLock {
            return branchNameToBranchChunkManager[branchName]
                ?: throw IllegalStateException("There is no Branch Chunk Manager for branch '${branchName}'!")
        }
    }

    fun getOrCreateChunkManagerForBranch(branchMetadata: IBranchMetadata): BranchChunkManager {
        this.branchDirectoryLock.writeLock().withLock {
            if (this.hasChunkManagerForBranch(branchMetadata.name)) {
                // manager exists; return it
                return this.getChunkManagerForBranch(branchMetadata.name)
            }
            // manager for branch does not exist; create it
            val branchDirName = when (branchMetadata.name) {
                ChronoDBConstants.MASTER_BRANCH_IDENTIFIER -> ChronoDBDirectoryLayout.MASTER_BRANCH_DIRECTORY
                else -> branchMetadata.directoryName
            }
            val branchDir = File(this.branchesDirectory, branchDirName)
            branchDir.createDirectoryIfNotExists()
            val bcm = BranchChunkManager.create(branchDir, branchMetadata.name)
            this.branchNameToBranchChunkManager[branchMetadata.name] = bcm
            return bcm
        }
    }

    fun getOrCreateChunkManagerForBranch(branch: Branch): BranchChunkManager {
        return this.getOrCreateChunkManagerForBranch(branch.metadata)
    }

    fun openReadOnlyTransactionOn(branch: String, timestamp: Long): ExodusChunkTransaction {
        return this.openTransactionOn(branch, timestamp, readOnly = true)
    }

    fun openReadWriteTransactionOn(branch: String, timestamp: Long): ExodusChunkTransaction {
        return this.openTransactionOn(branch, timestamp, readOnly = false)
    }

    fun openReadOnlyTransactionOn(chunk: ChronoChunk): ExodusChunkTransaction {
        return this.openTransactionOn(chunk, readOnly = true)
    }

    fun openReadWriteTransactionOn(chunk: ChronoChunk): ExodusChunkTransaction {
        return this.openTransactionOn(chunk, readOnly = false)
    }

    fun openReadOnlyTransactionOn(dataDirectory: File): ExodusTransaction {
        return this.openTransactionOn(dataDirectory, readOnly = true)
    }

    fun openReadWriteTransactionOn(dataDirectory: File): ExodusTransaction {
        return this.openTransactionOn(dataDirectory, readOnly = false)
    }

    fun openReadWriteTransactionOnHeadChunkOf(branchName: String): ExodusChunkTransaction {
        return this.openTransactionOnHeadChunkOf(branchName, readOnly = false)
    }

    fun openReadOnlyTransactionOnHeadChunkOf(branchName: String): ExodusChunkTransaction {
        return this.openTransactionOnHeadChunkOf(branchName, readOnly = true)
    }

    fun openReadOnlyTransactionOnGlobalEnvironment(): ExodusTransaction {
        return this.openTransactionOn(this.globalDirectory, readOnly = true)
    }

    fun openReadWriteTransactionOnGlobalEnvironment(): ExodusTransaction {
        return this.openTransactionOn(this.globalDirectory, readOnly = false)
    }

    fun getChunkBaseDataset(chunk: ChronoChunk, mode: ChunkBaseDataMode): List<ChronoDBEntry> {
        val keyspaceMetadata = this.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            NavigationIndex.getKeyspaceMetadata(tx, chunk.branchName)
        }
        val resultList = mutableListOf<ChronoDBEntry>()
        val lowerBound = chunk.validPeriod.lowerBound
        this.openTransactionOn(chunk, false).use { tx ->
            for (keyspace in keyspaceMetadata) {
                tx.withCursorOn(keyspace.matrixTableName) { cursor ->
                    while (cursor.next) {
                        val key = cursor.key.parseAsUnqualifiedTemporalKey()
                        if (mode == ChunkBaseDataMode.ALL || lowerBound == key.timestamp) {
                            resultList += ChronoDBEntryImpl.create(ChronoIdentifier.create(chunk.branchName, key.toTemporalKey(keyspace.keyspaceName)), cursor.value.toByteArray())
                        }
                    }
                }
            }
        }
        return resultList
    }

    @Suppress("unused")
    fun debugPrintChunkBaseDataset(chunk: ChronoChunk, mode: ChunkBaseDataMode) {
        val baseDataset = this.getChunkBaseDataset(chunk, mode)
        val msg = StringBuilder()
        msg.append("Base Data of $chunk (${chunk.validPeriod}):\n")
        if (baseDataset.isEmpty()) {
            println("\t<EMPTY>")
        }
        for (entry in baseDataset) {
            val entryValue: Any?
            if (entry.value != null) {
                if (entry.value.isEmpty()) {
                    entryValue = "<DELETE>"
                } else {
                    entryValue = KryoManager.deserialize(entry.value)
                }
            } else {
                entryValue = null
            }
            msg.append("\t${entry.identifier}, value: $entryValue\n")
        }
        log.info { msg.toString() }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun openTransactionOnHeadChunkOf(branchName: String, readOnly: Boolean): ExodusChunkTransaction {
        this.branchDirectoryLock.readLock().withLock {
            val bcm = this.getChunkManagerForBranch(branchName)
            val headChunk = bcm.headChunk
            return this.openTransactionOn(headChunk, readOnly = false)
        }
    }

    private fun openTransactionOn(branch: String, timestamp: Long, readOnly: Boolean): ExodusChunkTransaction {
        requireNonNegative(timestamp, "timestamp")
        this.branchDirectoryLock.readLock().withLock {
            val bcm = this.getChunkManagerForBranch(branch)
            if (bcm == null) {
                throw IllegalStateException("There is no branch named '${branch}'!")
            }
            val chunk = bcm.getChunkForTimestamp(timestamp)
            if (chunk == null) {
                throw IllegalStateException("There is no chunk in branch '${branch}' for timestamp ${timestamp}!")
            }
            return this.openTransactionOn(chunk, readOnly)
        }
    }

    private fun openTransactionOn(chunk: ChronoChunk, readOnly: Boolean): ExodusChunkTransaction {
        this.branchDirectoryLock.readLock().withLock {
            val exodusTx = openTransactionOn(chunk.dataDirectory, readOnly)
            return ExodusChunkTransactionImpl(exodusTx, chunk.validPeriod)
        }
    }

    private fun openTransactionOn(dataDirectory: File, readOnly: Boolean): ExodusTransaction {
        // requireDirectory(dataDirectory, "dataDirectory") // <-- performance hit too heavy!
        this.branchDirectoryLock.readLock().withLock {
            val env = this.environmentManager.getEnvironment(dataDirectory)
            val rawTx = if (readOnly) {
                env.beginReadonlyTransaction()
            } else {
                env.beginExclusiveTransaction()
            }
            return ExodusTransactionImpl(env, rawTx)
        }
    }

    fun compactAllExodusEnvironments() {
        this.branchDirectoryLock.readLock().withLock {
            this.branchNameToBranchChunkManager.values.forEach { bcm ->
                bcm.withReadLock {
                    val allChunks = bcm.getChunksForPeriod(Period.eternal())
                    for (chunk in allChunks) {
                        this.environmentManager.getEnvironment(chunk.dataDirectory).gc()
                        if (chunk.indexDirectory.exists()) {
                            this.environmentManager.getEnvironment(chunk.indexDirectory).gc()
                        }
                    }
                }
            }
            this.environmentManager.getEnvironment(this.globalDirectory).gc()
        }
    }

    fun compactAllExodusEnvironmentsForBranch(branchName: String) {
        this.branchDirectoryLock.readLock().withLock {
            val bcm = this.getChunkManagerForBranch(branchName)
            bcm.withReadLock {
                val allChunks = bcm.getChunksForPeriod(Period.eternal())
                for (chunk in allChunks) {
                    this.environmentManager.getEnvironment(chunk.dataDirectory).gc()
                    if (chunk.indexDirectory.exists()) {
                        this.environmentManager.getEnvironment(chunk.indexDirectory).gc()
                    }
                }
            }
        }
    }


    fun deleteBranch(branch: Branch) {
        this.branchDirectoryLock.writeLock().withLock {
            val bcm = this.getChunkManagerForBranch(branch.name)
            val branchDir = bcm.branchDirectory
            this.branchNameToBranchChunkManager.remove(branch.name)
            this.environmentManager.cleanupEnvironments(false)
            FileUtils.deleteDirectory(branchDir)
        }
    }

    enum class ChunkBaseDataMode {
        ALL, START_OF_CHUNK
    }

}
