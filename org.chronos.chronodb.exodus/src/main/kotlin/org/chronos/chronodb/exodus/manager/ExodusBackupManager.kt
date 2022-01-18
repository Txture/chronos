package org.chronos.chronodb.exodus.manager

import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.DumpOption
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat
import org.chronos.chronodb.api.dump.IncrementalBackupInfo
import org.chronos.chronodb.api.dump.IncrementalBackupResult
import org.chronos.chronodb.api.exceptions.ChronoDBBackupException
import org.chronos.chronodb.api.exceptions.ChronoDBException
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.internal.api.BranchInternal
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil
import org.chronos.chronodb.internal.impl.dump.ConverterRegistry
import org.chronos.chronodb.internal.impl.dump.DumpOptions
import org.chronos.chronodb.internal.impl.dump.entry.ChronoDBDumpEntry
import org.chronos.chronodb.internal.impl.dump.incremental.ChunkDumpMetadata
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata
import org.chronos.chronodb.internal.impl.dump.meta.CommitDumpMetadata
import org.chronos.chronodb.internal.impl.engines.base.AbstractBackupManager
import org.chronos.chronodb.internal.util.ChronosFileUtils
import org.chronos.common.version.ChronosVersion
import java.io.File
import java.nio.file.Files
import java.util.*

class ExodusBackupManager(owningDB: ExodusChronoDB) : AbstractBackupManager(owningDB) {

    private val owningDb: ExodusChronoDB
        get() = super.getOwningDb() as ExodusChronoDB

    @Override
    override fun createIncrementalBackup(minTimestamp: Long, lastRequestWallClockTime: Long): IncrementalBackupResult {
        require(minTimestamp >= 0) { "Precondition violation - argument 'minTimestamp' must not be negative (but is: ${minTimestamp})!" }
        require(lastRequestWallClockTime >= 0) { "Precondition violation - argument 'lastRequestWallClockTime' must not be negative (but is: ${lastRequestWallClockTime})!" }
        // create a temp directory where we prepare our CIB file content
        val tempDir = Files.createTempDirectory("chronosBackup").toFile()

        val filesDir = File(tempDir, "files")
        Files.createDirectory(filesDir.toPath())

        // export the actual chunk data
        this.owningDb.lockNonExclusive().use {
            // evaluate which chunks we need to export:
            // - chunks which have commits after "minTimestamp" OR
            // - chunks which are affected by dateback operations with a wall clock time after "lastRequestWallClockTime"
            this.owningDb.branchManager.branches.forEach { branch ->
                val datebackOperationsSinceLastRequest = this.owningDb.datebackManager.getDatebackOperationsPerformedBetween(
                        branch.name,
                        lastRequestWallClockTime,
                        System.currentTimeMillis()
                )
                val startTime = datebackOperationsSinceLastRequest.asSequence()
                        .map { it.earliestAffectedTimestamp }
                        .minOrNull().let {
                            if (it == null || it >= minTimestamp) {
                                minTimestamp
                            } else {
                                it
                            }
                        }
                if (branch.now >= startTime) {
                    // branch has changed since the last backup -> export
                    val chunkManager = this.owningDb.globalChunkManager.getChunkManagerForBranch(branch.name)
                    chunkManager.getChunksForPeriod(Period.createOpenEndedRange(startTime)).forEach { chunk ->
                        this.exportChunk(filesDir, branch, chunk)
                    }
                }
            }
        }

        // export the global data into an XML
        val globals = this.extractGlobalData()
        this.writeGlobalDataIntoFile(globals, File(filesDir, "global.xml"))

        // create the metadata
        val now = this.owningDb.branchManager.branches.asSequence().map { it.now }.maxOrNull() ?: 0
        val metadata = IncrementalBackupInfo(ChronosVersion.getCurrentVersion(), 2, minTimestamp, lastRequestWallClockTime, now, System.currentTimeMillis())
        // write it to a properties file
        this.writeBackupMetadataIntoFile(metadata, File(filesDir, "metadata.properties"))

        // calculate the SHA256 hash of all files we have written so far
        val sha256hash = ChronosFileUtils.sha256OfContentAndFileName(filesDir, filesDir.absolutePath).toString()

        // create file with hash as content
        val checksumFile = File(filesDir, "checksum.sha256")
        Files.createFile(checksumFile.toPath())
        checksumFile.writeText(sha256hash)

        // create an archive of everything in our directory
        val cibFileName = "chronos_backup_${metadata.requestStartTimestamp}to${metadata.now}at${metadata.wallClockTime}${ChronoDBConstants.INCREMENTAL_BACKUP_FILE_ENDING}"
        val cibFile = File(tempDir, cibFileName)
        ChronosFileUtils.createZipFile(filesDir, cibFile)

        // delete the "files" directory
        FileUtils.deleteDirectory(filesDir)

        return IncrementalBackupResult(cibFile, metadata)
    }

    override fun loadIncrementalBackups(cibFiles: List<File>) {
        val filteredList = cibFiles.asSequence()
                .filter { it.name.endsWith(ChronoDBConstants.INCREMENTAL_BACKUP_FILE_ENDING) }
                .filter { it.isFile }
                .toList()
        require(filteredList.isNotEmpty()) { "Precondition violation - no backup files given!" }
        this.owningDb.lockExclusive().use {
            if(this.owningDb.branchManager.masterBranch.now > 0){
                throw ChronoDBException("Can not load incremental backup on a non-empty database!")
            }
            val tmpDir = Files.createTempDirectory("ChronosBackupImport").toFile()
            try {
                val metadataToDir = filteredList.asSequence()
                        .mapIndexed { i, cibFile -> extractAndValidateCIBFile(tmpDir, i, cibFile) }
                        .sortedBy { it.first.now }
                        .toList()

                if(metadataToDir[0].first.requestStartTimestamp != 0L) {
                    throw ChronoDBException("The given set of incremental backup files is incomplete: The base file is missing!")
                }

                // check the backup files for gaps
                checkSequenceCompleteness(metadataToDir)

                // we made sure the backup is complete until the request timestamp, copy the chunks
                val mergedDir = File(tmpDir, "Merged")
                Files.createDirectory(mergedDir.toPath())
                metadataToDir.forEach { FileUtils.copyDirectory(it.second, mergedDir) }

                // we have all the chunks in the Merge-Directory, load the data
                val filesDir = File(mergedDir, "files")
                val globalXML = File(filesDir, "global.xml")
                val options = DumpOptions(DumpOption.FORCE_BINARY_ENCODING)
                val converterRegistry = ConverterRegistry(options)
                val metadata = ChronoDBDumpFormat.createInput(globalXML, DumpOptions()).use { input ->
                    input.next() as ChronoDBDumpMetadata
                }
                ChronoDBDumpUtil.createBranches(this.owningDb, metadata)
                ChronoDBDumpUtil.loadDatebackLog(this.owningDb, metadata)
                this.owningDb.branchManager.branches.forEach { branch ->
                    val branchDir = File(filesDir, branch.directoryName)
                    // all chunks in a branch have sequence numbers starting at 0
                    var sequenceNumber = 0
                    while(true){
                        val chunkXML = File(branchDir, "chunk${sequenceNumber}.xml")
                        if(!chunkXML.exists()){
                            break
                        }
                        ChronoDBDumpFormat.createInput(chunkXML, DumpOptions()).use { chunkInput ->
                            val chunkDumpMetadata = chunkInput.next() as ChunkDumpMetadata
                            val branchChunkManager = this.owningDb.globalChunkManager.getChunkManagerForBranch(branch.name)
                            branchChunkManager.createEmptyChunkFromBackup(chunkDumpMetadata)
                            val entries = chunkInput.asIterator().asSequence()
                                    .filterIsInstance<ChronoDBDumpEntry<*>>()
                                    .map { ChronoDBDumpUtil.convertDumpEntryToDBEntry(
                                            it,
                                            this.owningDb.serializationManager,
                                            converterRegistry
                                    ) }.toList()

                            this.owningDb.loadEntriesIntoChunks(entries)
                            val tkvs = (branch as BranchInternal).temporalKeyValueStore
                            chunkDumpMetadata.commitMetadata.forEach { commit -> tkvs.commitMetadataStore.put(commit.timestamp, commit.metadata) }
                            sequenceNumber++
                        }
                    }
                }

                // create secondary indices
                if (this.owningDb.requiresAutoReindexAfterDumpRead()) {
                    ChronoDBDumpUtil.setupIndexersAndReindex(this.owningDb, metadata)
                } else {
                    ChronoDBDumpUtil.setupIndexers(this.owningDb, metadata)
                }
            } catch(e: Exception){
                throw ChronoDBBackupException("Failed to load backup", e)
            } finally {
                tmpDir.deleteRecursively()
            }

        }

    }




    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private fun exportChunk(dumpDirectory: File, branch: Branch, chunk: ChronoChunk): File {
        val branchExportDir = File(dumpDirectory, branch.directoryName)
        if (!branchExportDir.exists()) {
            Files.createDirectory(branchExportDir.toPath())
        }
        val chunkExportFile = File(branchExportDir, "chunk${chunk.sequenceNumber}.xml")
        val options = DumpOptions(DumpOption.FORCE_BINARY_ENCODING)
        val converterRegistry = ConverterRegistry(options)
        ChronoDBDumpFormat.createOutput(chunkExportFile, DumpOptions()).use { output ->
            output.write(extractChunkMetadata(chunk, branch))
            val minTimestamp = chunk.validPeriod.lowerBound
            // the chunk upper bound is exclusive; the search algorithm is inclusive so we subtract 1 here.
            val maxTimestamp = chunk.validPeriod.upperBound - 1
            this.owningDb.entryStream(branch.name, minTimestamp, maxTimestamp).use { iterator ->
                ChronoDBDumpUtil.exportEntriesToDumpFormat(
                        output,
                        this.owningDb.serializationManager,
                        options.isForceBinaryEncodingEnabled,
                        converterRegistry,
                        iterator
                )
            }
        }
        return chunkExportFile
    }

    private fun extractChunkMetadata(chunk: ChronoChunk, branch: Branch): ChunkDumpMetadata {
        val upperBound = Math.min(chunk.validPeriod.upperBound.let {
            if (it < Long.MAX_VALUE) {
                it + 1
            } else {
                it
            }
        }, branch.now)
        val commitMetadata = this.owningDb.tx().getCommitMetadataBetween(chunk.validPeriod.lowerBound, upperBound).asSequence()
                .map { entry -> CommitDumpMetadata(branch.name, entry.key, entry.value) }
                .toList()
        val chunkDumpMetadata = ChunkDumpMetadata(branch.name, chunk.sequenceNumber, chunk.validPeriod, commitMetadata)
        return chunkDumpMetadata
    }

    private fun writeBackupMetadataIntoFile(metadata: IncrementalBackupInfo, file: File) {
        val properties = Properties()
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_CHRONOS_VERSION, metadata.chronosVersion.toString())
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_FORMAT_VERSION, metadata.dumpFormatVersion.toString())
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_START_TIMESTAMP, metadata.requestStartTimestamp.toString())
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_PREVIOUS_WALL_CLOCK_TIME, metadata.previousRequestWallClockTime.toString())
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_WALL_CLOCK_TIME, metadata.wallClockTime.toString())
        properties.put(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_NOW, metadata.now.toString())
        Files.deleteIfExists(file.toPath())
        Files.createFile(file.toPath())
        file.bufferedWriter().use { writer ->
            properties.store(writer, "")
        }
    }

    private fun extractGlobalData(): ChronoDBDumpMetadata {
        return ChronoDBDumpUtil.extractMetadata(this.owningDb, false)
    }

    private fun writeGlobalDataIntoFile(globals: ChronoDBDumpMetadata, file: File) {
        Files.deleteIfExists(file.toPath())
        Files.createFile(file.toPath())
        ChronoDBDumpFormat.createOutput(file, DumpOptions()).use { output ->
            output.write(globals)
        }
    }

    private fun extractAndValidateCIBFile(tmpDir: File?, i: Int, cibFile: File): Pair<IncrementalBackupInfo, File> {
        val targetDirectory = File(tmpDir, i.toString())
        Files.createDirectory(targetDirectory.toPath())
        ChronosFileUtils.extractZipFile(cibFile, targetDirectory)
        val filesDir = File(targetDirectory, "files")
        val checksumFile = File(filesDir, "checksum.sha256")
        if (!checksumFile.exists()) {
            throw ChronoDBBackupException("Backup file format of ${cibFile.absolutePath} is invalid!")
        }
        val sha256 = checksumFile.readText()
        Files.delete(checksumFile.toPath())
        val sha256Calculated = ChronosFileUtils.sha256OfContentAndFileName(filesDir, filesDir.absolutePath).toString()
        if (sha256 != sha256Calculated) {
            throw ChronoDBBackupException("Backup file ${cibFile.absolutePath} has been corrupted. Checksum does not match!")
        }
        val metadataFile = File(filesDir, "metadata.properties") //must exist since checksum is ok
        val properties = Properties()
        properties.load(metadataFile.bufferedReader())
        val formatVersion = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_FORMAT_VERSION) as String
        if (formatVersion != "2") {
            throw ChronoDBBackupException("Backup file ${cibFile.absolutePath} has unknown format version ${formatVersion}. Maybe it was created by a newer version of Chronos?")
        }
        val chronosVersion = ChronosVersion.parse(properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_CHRONOS_VERSION) as String)
        val now = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_NOW) as String
        val responseWallClockTime = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_RESPONSE_WALL_CLOCK_TIME) as String
        val requestPreviousWallClockTime = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_PREVIOUS_WALL_CLOCK_TIME) as String
        val requestStartTimestamp = properties.get(ChronoDBConstants.IncrementalBackup.METADATA_KEY_REQUEST_START_TIMESTAMP) as String
        val metadata = IncrementalBackupInfo(chronosVersion, formatVersion.toInt(),
                requestStartTimestamp.toLong(), requestPreviousWallClockTime.toLong(),
                now.toLong(), responseWallClockTime.toLong()
        )
        return metadata to targetDirectory
    }

    private fun checkSequenceCompleteness(metadataToDir: List<Pair<IncrementalBackupInfo, File>>) {
        for (i in 1 until metadataToDir.size) {
            val previousEnd = metadataToDir[i - 1].first.now
            val currentStart = metadataToDir[i].first.requestStartTimestamp
            if (previousEnd != currentStart) {
                throw ChronoDBBackupException("The given set of incremental backup files is incomplete: gap detected!")
            }
        }
    }
}