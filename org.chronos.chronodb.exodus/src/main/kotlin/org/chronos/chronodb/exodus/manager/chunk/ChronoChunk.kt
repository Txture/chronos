package org.chronos.chronodb.exodus.manager.chunk

import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.kotlin.ext.createDirectoryIfNotExists
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.kotlin.ext.requireDirectory
import org.chronos.chronodb.exodus.kotlin.ext.requireExistingDirectory
import org.chronos.chronodb.internal.api.Period
import java.io.File
import java.nio.file.Files

class ChronoChunk {

    object Comparators {

        val BY_VALID_FROM_ASCENDING: Comparator<ChronoChunk> = Comparator.comparing { chunk: ChronoChunk -> chunk.validPeriod }


    }

    // =================================================================================================================
    // FACTORY
    // =================================================================================================================

    companion object {

        /**
         * Creates a new chunk on disk.
         *
         * This method will create the required directory layout and files.
         *
         * @param chunkDirectory The directory where the chunk should reside. <b>WILL BE CLEARED!</b>
         * @param metadata The metadata for the branch
         * @return The newly created chunk
         */
        @JvmStatic
        fun createNewChunk(chunkDirectory: File, metadata: ChunkMetadata): ChronoChunk {
            if(chunkDirectory.exists()) {
                requireDirectory(chunkDirectory, "chunkDirectory")
                // if the directory exists, clear all contained files
                FileUtils.cleanDirectory(chunkDirectory)
            }else{
                Files.createDirectory(chunkDirectory.toPath())
            }
            // create the data directory
            val dataDirectory = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_DATA_DIRECTORY)
            Files.createDirectory(dataDirectory.toPath())
            // create and populate the metadata directory
            val metaDirectory = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_METADATA_DIRECTORY)
            Files.createDirectory(metaDirectory.toPath())
            metadata.writeBinaryTo(metaDirectory)
            // create the chunk info properties file
            val chunkInfoFile = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_INFO_PROPERTIES)
            Files.createFile(chunkInfoFile.toPath())
            metadata.writePlainTextTo(chunkInfoFile)
            // return the chunk object
            return ChronoChunk(chunkDirectory, metadata)
        }

        /**
         * Attempts to [readExistingChunk], but returns `null` instead of throwing an exception.
         *
         * @param chunkDirectory The chunk directory. Must be an existing directory.
         * @return The loaded chunk, or `null` if an error occurred during loading.
         */
        @JvmStatic
        fun tryReadExistingChunk(chunkDirectory: File): ChronoChunk? {
            try{
                return readExistingChunk(chunkDirectory)
            }catch(e: Exception){
                return null
            }
        }

        /**
         * Reads an existing chunk from disk.
         *
         * This method assumes that a chunk lock file exists in the given directory. If there
         * is no chunk lock file, this method will throw an [IllegalStateException].
         *
         * @param chunkDirectory The directory where the chunk is located. Must be a directory.
         *
         * @return The loaded chunk instance.
         */
        @JvmStatic
        fun readExistingChunk(chunkDirectory: File): ChronoChunk {
            requireExistingDirectory(chunkDirectory, "chunkDirectory")
            val chunkLockFile = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_LOCK_FILE)
            if (!chunkLockFile.exists() || !chunkLockFile.isFile) {
                throw IllegalStateException("The given chunk directory contains no chunk lock file: ${chunkDirectory.absolutePath}")
            }
            val dataDir = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_DATA_DIRECTORY)
            if (!dataDir.exists() || !dataDir.isDirectory) {
                throw IllegalStateException("The given chunk directory contains no data directory: ${chunkDirectory.absolutePath}")
            }
            val metaDir = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_METADATA_DIRECTORY)
            if (!metaDir.exists() || !metaDir.isDirectory) {
                throw IllegalStateException("The given chunk directory contains no meta directory: ${chunkDirectory.absolutePath}")
            }
            val metadata = ChunkMetadata.readFrom(metaDir)
            return ChronoChunk(chunkDirectory, metadata)
        }

    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    val chunkDirectory: File
    val lockFile: File
    val indexDirectory: File
    val metaDirectory: File
    val dataDirectory: File

    private val metadata: ChunkMetadata

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    private constructor(chunkDirectory: File, metadata: ChunkMetadata) {
        requireExistingDirectory(chunkDirectory, "chunkDirectory")
        this.dataDirectory = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_DATA_DIRECTORY)
        if (!this.dataDirectory.exists()) {
            throw IllegalStateException("Chunk data directory does not exist: ${this.dataDirectory}")
        }
        this.metaDirectory = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_METADATA_DIRECTORY)
        if (!this.metaDirectory.exists()) {
            throw IllegalStateException("Chunk meta directory does not exist: ${this.metaDirectory}")
        }
        this.indexDirectory = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_INDEX_DIRCTORY)
        this.indexDirectory.createDirectoryIfNotExists()
        this.lockFile = File(chunkDirectory, ChronoDBDirectoryLayout.CHUNK_LOCK_FILE)
        this.chunkDirectory = chunkDirectory
        this.metadata = metadata
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    val validPeriod: Period
        get() = this.metadata.validPeriod

    val branchName: String
        get() = this.metadata.branchName

    val sequenceNumber: Long
        get() = this.metadata.sequenceNumber

    val isHeadChunk: Boolean
        get() = this.metadata.validTo >= java.lang.Long.MAX_VALUE

    /**
     * Creates the chunk lock file. This is an atomic operation in most file systems.
     *
     * @return `true` if the chunk lock file has been created, `false` if it already existed.
     */
    fun createChunkLockFile(): Boolean {
        if (this.lockFile.exists()) {
            return false
        } else {
            Files.createFile(this.lockFile.toPath())
            return true
        }
    }

    fun setValidToTimestampTo(upperBound: Long) {
        require(upperBound > this.metadata.validFrom) { "Precondition violation - argument 'upperBound' must be greater than 'validFrom'!"}
        this.metadata.validTo = upperBound
        this.metadata.writeBinaryTo(this.metaDirectory)
        this.metadata.writePlainTextTo(File(this.chunkDirectory, ChronoDBDirectoryLayout.CHUNK_INFO_PROPERTIES))
    }

    /**
     * Checks if the given chunk is the first in the chunk series for a non-master branch.
     *
     * Those chunks contain the delta to their origin branch at the branching timestamp. After the first rollover, they will contain the full information.
     *
     * @param this@isBranchDeltaChunk The chunk to check.
     * @return `true` if the given chunk is a delta chunk, i.e. the first chunk after a branching operation, otherwise `false`.
     */
    val isDeltaChunk: Boolean
        get() {
            if (ChronoDBConstants.MASTER_BRANCH_IDENTIFIER == this.branchName) {
                // the master branch has no delta chunks
                return false
            }
            // the first chunk in each branch is the delta chunk
            return sequenceNumber == 0L
        }

    override fun toString(): String {
        return "Chunk[${this.branchName} -> ${this.sequenceNumber}]"
    }

}