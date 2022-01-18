package org.chronos.chronodb.exodus.manager.chunk

import com.google.common.collect.Iterators
import com.google.common.collect.Maps
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.exodus.ExodusDataMatrixUtil
import org.chronos.chronodb.exodus.kotlin.ext.requireExistingDirectory
import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.dump.incremental.ChunkDumpMetadata
import org.chronos.common.version.ChronosVersion
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock

class BranchChunkManager {


    companion object {

        const val BRANCH_INFO_PROPERTIES__BRANCH_NAME = "branchName"

        @JvmStatic
        fun create(branchDirectory: File, branchName: String): BranchChunkManager {
            requireExistingDirectory(branchDirectory, "branchDirectory")
            val periodToChunk = scanDirectoryForChunks(branchDirectory)
            // check that chunk periods do not intersect...
            checkForPeriodIntersections(periodToChunk.keys, branchName, branchDirectory)
            // ... and that all chunks actually belong to this branch
            checkForChunksFromForeignBranches(periodToChunk.values, branchName, branchDirectory)
            // assert that a head chunk exists
            createHeadRevisionChunkIfNecessary(branchDirectory, branchName, periodToChunk)
            createBranchInfoPropertiesFile(branchDirectory, branchName)
            // everything seems to be okay, open the manager
            return BranchChunkManager(branchDirectory, branchName, periodToChunk)
        }


        private fun scanDirectoryForChunks(branchDirectory: File): NavigableMap<Period, ChronoChunk> {
            val resultMap = Maps.newTreeMap<Period, ChronoChunk>()
            branchDirectory.listFiles().asSequence()
                    .filter(File::isDirectory)
                    .filter { it.name.matches(ChronoDBDirectoryLayout.CHUNK_DIRECTORY_REGEX.toRegex()) }
                    .filter { it.containsChunkLockFile() }
                    .map(ChronoChunk.Companion::tryReadExistingChunk)
                    .filter(Objects::nonNull)
                    .forEach { chunk -> resultMap[chunk!!.validPeriod] = chunk }
            return resultMap
        }

        private fun checkForPeriodIntersections(periods: Iterable<Period>, branchName: String, branchDirectory: File) {
            val periodIterator = Iterators.peekingIterator(periods.iterator())
            var periodIndex = 0
            while (periodIterator.hasNext()) {
                val currentPeriod = periodIterator.next()
                if (periodIterator.hasNext()) {
                    val nextPeriod = periodIterator.peek()
                    if (currentPeriod.overlaps(nextPeriod)) {
                        throw IllegalStateException("There is a chunk validity period overlap! " +
                                "Branch: '${branchName}', directory: '${branchDirectory.name}'. " +
                                "Offending chunk indices are ${periodIndex} (valid period: ${currentPeriod}) " +
                                "and ${periodIndex + 1} (valid period: ${nextPeriod}).")
                    }
                }
                periodIndex++
            }
        }

        private fun checkForChunksFromForeignBranches(chunks: Collection<ChronoChunk>, branchName: String, branchDirectory: File) {
            for (chunk in chunks) {
                if (chunk.branchName != branchName) {
                    throw IllegalStateException("Detected a stray chunk from a different branch! " +
                            "Branch '${branchName}', directory: '${branchDirectory.name}'. " +
                            "Offending chunk (chunk directory: '${chunk.chunkDirectory.name}') belongs to another branch ('${chunk.branchName}')!")
                }
            }
        }

        private fun createHeadRevisionChunkIfNecessary(branchDirectory: File, branchName: String, periodToChunk: NavigableMap<Period, ChronoChunk>) {
            val lastEntry = periodToChunk.lastEntry()
            if (lastEntry == null) {
                try {
                    // no chunks, create an initial (open-ended) one
                    // first, clear any existing files
                    val chunkDir = clearAllFilesOfChunk(branchDirectory, 0)
                    // create meta data
                    val metaData = ChunkMetadata(
                            validFrom = 0,
                            validTo = Long.MAX_VALUE,
                            branchName = branchName,
                            sequenceNumber = 0
                    )
                    // create chunk and place it in our routing map
                    val chunk = ChronoChunk.createNewChunk(chunkDir, metaData)
                    periodToChunk[metaData.validPeriod] = chunk
                    chunk.createChunkLockFile()
                } catch (e: IOException) {
                    throw IllegalStateException("Failed to create head revision chunk!", e)
                }
            } else {
                val period = lastEntry.key
                if (!period.isOpenEnded) {
                    // we found a chunk and it is not open ended, open-up period
                    periodToChunk.remove(period)
                    val chunk = lastEntry.value
                    chunk.setValidToTimestampTo(Long.MAX_VALUE)
                    periodToChunk[chunk.validPeriod] = chunk
                }
            }
        }

        private fun createBranchInfoPropertiesFile(branchDirectory: File, branchName: String) {
            val branchInfoFile = File(branchDirectory, ChronoDBDirectoryLayout.BRANCH_INFO_PROPERTIES)
            if (branchInfoFile.exists()) {
                // branch info file already exists, check the content
                val properties = Properties()
                branchInfoFile.bufferedReader().use { reader ->
                    properties.load(reader)
                }
                val storedBranchName = properties.getProperty(BRANCH_INFO_PROPERTIES__BRANCH_NAME)
                if (storedBranchName == branchName) {
                    // file is consistent with our branch, nothing to do
                    return
                }
            }
            // branch info file does not exist, create it
            val properties = Properties()
            properties.setProperty(BRANCH_INFO_PROPERTIES__BRANCH_NAME, branchName)
            branchInfoFile.printWriter().use { writer ->
                writer.write("# BRANCH INFORMATION written by Chronos ${ChronosVersion.getCurrentVersion()}\n")
                writer.write("# This file is intended as a human-readable information source.\n")
                writer.write("# The contents of this file will be overwritten when needed. DO NOT MODIFY IT.\n")
                writer.write("# Last modification date: ${Date()}\n")
                writer.write("# ============================================================================\n")
                writer.write("\n")
                properties.store(writer, null)
                writer.flush()
            }
        }

    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    val branchDirectory: File
    val branchName: String

    private val accessLock = ReentrantReadWriteLock(true)
    private val periodToChunk: NavigableMap<Period, ChronoChunk>

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    private constructor(branchDirectory: File, branchName: String, periodToChunk: NavigableMap<Period, ChronoChunk>) {
        requireExistingDirectory(branchDirectory, "branchDirectory")
        this.branchName = branchName
        this.branchDirectory = branchDirectory
        this.periodToChunk = periodToChunk
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    val headChunk: ChronoChunk
        get() {
            this.accessLock.readLock().withLock {
                val headChunk = this.periodToChunk.lastEntry()?.value
                if (headChunk == null || !headChunk.isHeadChunk) {
                    throw IllegalStateException("There is no valid head chunk in branch '${this.branchName}'!")
                }
                return headChunk
            }
        }

    /**
     * Gets the which contains the data for the given timestamp.
     *
     * @param timestamp The timestamp to get the chunk for. Must not be negative.
     * @return The chunk containing the timestamp, or `null` if no chunk matched.
     */
    fun getChunkForTimestamp(timestamp: Long): ChronoChunk? {
        requireNonNegative(timestamp, "timestamp")
        this.accessLock.readLock().withLock {
            val chunkIterator = Iterators.peekingIterator(this.periodToChunk.values.iterator())
            while (chunkIterator.hasNext()) {
                val chunk = chunkIterator.next()
                if (chunk.validPeriod.contains(timestamp)) {
                    // exact match
                    return chunk
                }
                if (chunkIterator.hasNext()) {
                    val nextChunk = chunkIterator.peek()
                    if (chunk.validPeriod.isBefore(timestamp) && nextChunk.validPeriod.isAfter(timestamp)) {
                        // the requested timestamp falls into a "hole" in our chunks: the current one
                        // ends too soon, and the next one starts after the timestamp. In this case,
                        // we redirect to the next-lower chunk.
                        return chunk
                    }
                }
            }
        }
        // no chunk found. This usually means that the first chunk in this branch starts after
        // the request timestamp.
        return null
    }

    /**
     * Gets all chunks within the given period in ascending order.
     *
     * @param period The period to get all overlapping and contained chunks of. Must not be `null`.
     * @return The list of chunks in the period in ascending order. Maybe empty, never `null`.
     */
    fun getChunksForPeriod(period: Period): List<ChronoChunk> {
        return this.accessLock.readLock().withLock {
            this.periodToChunk.values.asSequence()
                    .filter { it.validPeriod.overlaps(period) }
                    .toList()
        }
    }

    /**
     * Gets all chunks within this branch in ascending order.
     *
     * @return The list of chunks in the period in ascending order. Maybe empty, never `null`.
     */
    fun getAllChunks(): List<ChronoChunk>{
        return this.accessLock.read {
            this.periodToChunk.values.toList()
        }
    }

    /**
     * Terminates the validity range of the current head chunk at the given timestamp and opens a new head chunk.
     *
     * @param rolloverTimestamp The timestamp at which to perform the rollover. Must be larger than the start timestamp of the current head chunk.
     * @param dataTransfer Transfers the chunk content data from the previous head chunk to the new one.
     * @return The new head chunk.
     */
    fun performRollover(rolloverTimestamp: Long, dataTransfer: (RolloverProcessInfo) -> Unit): ChronoChunk {
        requireNonNegative(rolloverTimestamp, "rolloverTimestamp")
        this.accessLock.writeLock().withLock {
            val oldHeadChunk = this.headChunk
            val oldHeadChunkPeriod = oldHeadChunk.validPeriod
            if (oldHeadChunkPeriod.lowerBound >= rolloverTimestamp) {
                throw IllegalArgumentException("The given rollover timestamp (${rolloverTimestamp}) is less than the head chunk lower bound (${oldHeadChunkPeriod.lowerBound})!")
            }
            // calculate the new chunk sequence number
            val sequenceNumber = oldHeadChunk.sequenceNumber + 1
            // delete all existing files that could conflict with the new chunk
            val newChunkDir = clearAllFilesOfChunk(this.branchDirectory, sequenceNumber)
            // terminate the old period at the given timestamp
            oldHeadChunk.setValidToTimestampTo(rolloverTimestamp)
            val newMetaData = ChunkMetadata(
                    validFrom = rolloverTimestamp,
                    validTo = Long.MAX_VALUE,
                    branchName = this.branchName,
                    sequenceNumber = sequenceNumber
            )
            val newChunk = ChronoChunk.createNewChunk(newChunkDir, newMetaData)
            dataTransfer(
                    RolloverProcessInfo(
                            oldHeadChunk = oldHeadChunk,
                            newHeadChunk = newChunk,
                            rolloverTimestamp = rolloverTimestamp
                    )
            )
            newChunk.createChunkLockFile()
            // update our file map
            this.periodToChunk.remove(oldHeadChunkPeriod)
            this.periodToChunk[oldHeadChunk.validPeriod] = oldHeadChunk
            // create the new head revision chunk
            val newHeadRevisionPeriod = Period.createOpenEndedRange(rolloverTimestamp)
            this.periodToChunk[newHeadRevisionPeriod] = newChunk
            return newChunk
        }
    }

    fun createEmptyChunkFromBackup(chunkDumpMetadata: ChunkDumpMetadata): ChronoChunk {
        require(chunkDumpMetadata.branchName == this.branchName) { "Precondition violation - the given chunk does not belong to this branch!" }
        val chunkSequenceNumber = chunkDumpMetadata.chunkSequenceNumber
        val validPeriod = chunkDumpMetadata.validPeriod
        val chunkMetadata = ChunkMetadata(validPeriod.lowerBound, validPeriod.upperBound, chunkDumpMetadata.branchName, chunkSequenceNumber)
        val chunkDir = clearAllFilesOfChunk(this.branchDirectory, chunkSequenceNumber)
        val chunk = ChronoChunk.createNewChunk(chunkDir, chunkMetadata)
        chunk.createChunkLockFile()
        if(validPeriod.lowerBound == 0L){
            this.periodToChunk.clear()
        }
        this.periodToChunk[validPeriod] = chunk
        return chunk
    }

    fun withReadLock(function: () -> Unit) {
        this.accessLock.readLock().withLock(function)
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================


}

// =================================================================================================================
// MISCELLANEOUS FUNCTIONS
// =================================================================================================================


private fun clearAllFilesOfChunk(branchDirectory: File, chunkNumber: Long): File {
    val chunkDir = File(branchDirectory, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + chunkNumber)
    if (chunkDir.exists()) {
        FileUtils.cleanDirectory(chunkDir)
    } else {
        Files.createDirectory(chunkDir.toPath())
    }
    return chunkDir
}

private fun File.containsChunkLockFile(): Boolean {
    return File(this, ChronoDBDirectoryLayout.CHUNK_LOCK_FILE).exists()
}