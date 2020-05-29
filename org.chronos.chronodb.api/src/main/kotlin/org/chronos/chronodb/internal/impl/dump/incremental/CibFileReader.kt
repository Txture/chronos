package org.chronos.chronodb.internal.impl.dump.incremental

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat
import org.chronos.chronodb.api.dump.IncrementalBackupInfo
import org.chronos.chronodb.api.exceptions.ChronoDBException
import org.chronos.chronodb.internal.impl.dump.DumpOptions
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata
import org.chronos.chronodb.internal.util.ChronosFileUtils
import org.chronos.common.version.ChronosVersion
import java.io.File
import java.nio.file.Files
import java.util.*

class CibFileReader : AutoCloseable {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private val cibFile: File
    private val rootDir: File
    private val filesDir: File
    private var closed = false

    val globalData: ChronoDBDumpMetadata by lazy { this.loadGlobalDataXml() }
    val incrementalBackupInfo: IncrementalBackupInfo by lazy { this.loadIncrementalBackupInfo() }

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(cibFile: File) {
        require(cibFile.exists()) { "Precondition violation - argument 'cibFile' referes to non-existing file! Path: ${cibFile.absolutePath}" }
        require(cibFile.isFile) { "Precondition violation - argument 'cibFile' must be a file (not a directory)! Path: ${cibFile.absolutePath}" }
        this.cibFile = cibFile
        this.rootDir = Files.createTempDirectory("cibReader").toFile()
        ChronosFileUtils.extractZipFile(cibFile, rootDir)
        this.filesDir = File(rootDir, "files")
        val checksumFile = File(filesDir, "checksum.sha256")
        if (!checksumFile.exists()) {
            throw ChronoDBException("Backup file format of ${cibFile.absolutePath} is invalid!")
        }
        val sha256 = checksumFile.readText()
        Files.delete(checksumFile.toPath())
        val sha256Calculated = ChronosFileUtils.sha256OfContentAndFileName(filesDir, filesDir.absolutePath).toString()
        if (sha256 != sha256Calculated) {
            throw ChronoDBException("Backup file ${cibFile.absolutePath} has been corrupted. Checksum does not match!")
        }
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    fun readChunkMetadata(branchName: String, chunkSequenceNumber: Long): ChunkDumpMetadata {
        require(chunkSequenceNumber >= 0) { "Precondition violation - argument 'chunkSequenceNumber' must not be negative (is ${chunkSequenceNumber})." }
        assertNotClosed()
        val branchDir = this.getBranchDir(branchName)
        val chunkXml = File(branchDir, "chunk${chunkSequenceNumber}.xml")
        return ChronoDBDumpFormat.createInput(chunkXml, DumpOptions()).use { input ->
            input.next() as ChunkDumpMetadata
        }
    }

    fun readChunkContents(branchName: String, chunkSequenceNumber: Long): List<ChronoDBDumpEntry<*>> {
        assertNotClosed()
        val branchDir = this.getBranchDir(branchName)
        val chunkXml = File(branchDir, "chunk${chunkSequenceNumber}.xml")
        return ChronoDBDumpFormat.createInput(chunkXml, DumpOptions()).use { input ->
            // discard the globals entry
            input.next()
            val resultList = mutableListOf<ChronoDBDumpEntry<*>>()
            input.forEachRemaining { resultList.add(it as ChronoDBDumpEntry<*>) }
            resultList
        }
    }

    fun listContainedChunksForMasterBranch(): List<Long>{
        assertNotClosed()
        return this.listContainedChunksForBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)
    }

    fun listContainedChunksForBranch(branchName: String): List<Long> {
        assertNotClosed()
        val branchDir = this.getBranchDir(branchName)
        if (!branchDir.exists()) {
            return emptyList()
        }
        return branchDir.listFiles { f -> f.name.startsWith("chunk") && f.name.endsWith(".xml") }.asSequence()
                .map { it.name }
                .map { it.removePrefix("chunk").removeSuffix(".xml").toLong() }
                .toList()
    }

    override fun close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.rootDir.deleteRecursively()
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun loadGlobalDataXml(): ChronoDBDumpMetadata {
        assertNotClosed()
        val globalsXml = File(this.filesDir, "global.xml")
        return ChronoDBDumpFormat.createInput(globalsXml, DumpOptions()).use { input ->
            input.next() as ChronoDBDumpMetadata
        }
    }

    private fun loadIncrementalBackupInfo(): IncrementalBackupInfo {
        assertNotClosed()
        val propertiesFile = File(this.filesDir, "metadata.properties")
        val properties = Properties()
        properties.load(propertiesFile.bufferedReader())
        val formatVersion = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_FORMAT_VERSION) as String
        if (formatVersion != "2") {
            throw ChronoDBException("Backup file ${this.cibFile} has unknown format version ${formatVersion}. Maybe it was created by a newer version of Chronos?")
        }
        val chronosVersion = ChronosVersion.parse(properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_CHRONOS_VERSION) as String)
        val now = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_NOW) as String
        val responseWallClockTime = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_WALL_CLOCK_TIME) as String
        val requestPreviousWallClockTime = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_PREVIOUS_WALL_CLOCK_TIME) as String
        val requestStartTimestamp = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_START_TIMESTAMP) as String
        return IncrementalBackupInfo(chronosVersion, formatVersion.toInt(),
                requestStartTimestamp.toLong(), requestPreviousWallClockTime.toLong(),
                now.toLong(), responseWallClockTime.toLong()
        )
    }

    private fun getBranchDirName(branchName: String): String {
        assertNotClosed()
        return this.globalData.branchDumpMetadata.asSequence()
                .filter { it.name == branchName }
                .map { it.directoryName }
                .firstOrNull() ?: throw IllegalArgumentException("The dump contains no branch named '${branchName}'!")
    }

    private fun getBranchDir(branchName: String): File {
        assertNotClosed()
        return File(this.filesDir, getBranchDirName(branchName))
    }

    private fun assertNotClosed() {
        if (this.closed) {
            throw IllegalStateException("CibFileReader is already closed!")
        }
    }


}