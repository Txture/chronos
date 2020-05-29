package org.chronos.chronodb.exodus.test.cases.chunk

import org.chronos.chronodb.exodus.kotlin.ext.onlyElement
import org.chronos.chronodb.exodus.manager.chunk.BranchChunkManager
import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.manager.chunk.ChunkMetadata
import org.chronos.chronodb.exodus.test.base.TestWithTempDir
import org.chronos.common.testing.kotlin.ext.notBeNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.internal.api.Period
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.lang.IllegalStateException
import java.util.*

class BranchChunkManagerTest : TestWithTempDir() {

    @Test
    fun canOpenManagerOnEmptyDirectory() {
        val bcm = BranchChunkManager.create(this.testDir, "test")
        bcm.branchName shouldBe "test"
        bcm.branchDirectory shouldBe this.testDir

        // this should (at the very least) create a branch info file
        val branchInfoFile = File(this.testDir, ChronoDBDirectoryLayout.BRANCH_INFO_PROPERTIES)
        branchInfoFile.exists() shouldBe true

        // read the branch info properties file
        val properties = Properties()
        branchInfoFile.bufferedReader().use { reader -> properties.load(reader) }
        val branchName = properties.getProperty(BranchChunkManager.BRANCH_INFO_PROPERTIES__BRANCH_NAME)
        branchName shouldBe "test"
    }

    @Test
    fun openingOnEmptyDirectoryCreatesHeadChunk() {
        val bcm = BranchChunkManager.create(this.testDir, "test")
        bcm.headChunk should notBeNull()

        val headChunkDir = bcm.branchDirectory.listFiles().asSequence()
            .filter { it.name.startsWith(ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX) }
            .toList().onlyElement()
        headChunkDir shouldBe bcm.headChunk.chunkDirectory
        headChunkDir.exists() shouldBe true
        bcm.headChunk.branchName shouldBe "test"
        bcm.headChunk.validPeriod shouldBe Period.createOpenEndedRange(0)
        bcm.headChunk.sequenceNumber shouldBe 0
        bcm.headChunk.lockFile.exists() shouldBe true
    }

    @Test
    fun openingManagerDetectsAllPresentChunks() {
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk1Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1)
        val chunk2Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 2)
        val chunk0Meta = ChunkMetadata(validFrom = 0, validTo = 1000, sequenceNumber = 0, branchName = "master")
        val chunk1Meta = ChunkMetadata(validFrom = 1000, validTo = 5000, sequenceNumber = 1, branchName = "master")
        val chunk2Meta = ChunkMetadata(validFrom = 5000, validTo = Long.MAX_VALUE, sequenceNumber = 2, branchName = "master")
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk1Dir, chunk1Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk2Dir, chunk2Meta).createChunkLockFile()

        val bcm = BranchChunkManager.create(this.testDir, "master")

        val chunks = bcm.getChunksForPeriod(Period.createOpenEndedRange(0))
        chunks.size shouldBe 3
        chunks[0].sequenceNumber shouldBe 0; chunks[0].branchName shouldBe "master"
        chunks[1].sequenceNumber shouldBe 1; chunks[1].branchName shouldBe "master"
        chunks[2].sequenceNumber shouldBe 2; chunks[2].branchName shouldBe "master"
    }

    @Test
    fun openingManagerSkipsOverChunksWithoutLockFile() {
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk1Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1)
        val chunk2Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 2)
        val chunk0Meta = ChunkMetadata(validFrom = 0, validTo = 1000, sequenceNumber = 0, branchName = "master")
        val chunk1Meta = ChunkMetadata(validFrom = 1000, validTo = 5000, sequenceNumber = 1, branchName = "master")
        val chunk2Meta = ChunkMetadata(validFrom = 5000, validTo = Long.MAX_VALUE, sequenceNumber = 2, branchName = "master")
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk1Dir, chunk1Meta) // no lock file here!
        ChronoChunk.createNewChunk(chunk2Dir, chunk2Meta).createChunkLockFile()

        val bcm = BranchChunkManager.create(this.testDir, "master")

        val chunks = bcm.getChunksForPeriod(Period.createOpenEndedRange(0))
        chunks.size shouldBe 2
        chunks[0].sequenceNumber shouldBe 0; chunks[0].branchName shouldBe "master"
        chunks[1].sequenceNumber shouldBe 2; chunks[1].branchName shouldBe "master"
    }

    @Test
    fun openingManagerDetectsChunkPeriodOverlaps() {
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk1Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1)
        val chunk2Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 2)
        val chunk0Meta = ChunkMetadata(validFrom = 0, validTo = 1000, sequenceNumber = 0, branchName = "master")
        val chunk1Meta = ChunkMetadata(validFrom = 1000, validTo = 5000, sequenceNumber = 1, branchName = "master")
        val chunk2Meta = ChunkMetadata(
                validFrom = 4999, // overlap with previous chunk!
                validTo = Long.MAX_VALUE,
                sequenceNumber = 2,
                branchName = "master"
        )
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk1Dir, chunk1Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk2Dir, chunk2Meta).createChunkLockFile()

        assertThrows<IllegalStateException> {
            BranchChunkManager.create(this.testDir, "master")
        }
    }

    @Test
    fun openingManagerDetectsForeignBranchChunks() {
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk1Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1)
        val chunk2Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 2)
        val chunk0Meta = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                sequenceNumber = 0,
                branchName = "other" // not "master"!
        )
        val chunk1Meta = ChunkMetadata(validFrom = 1000, validTo = 5000, sequenceNumber = 1, branchName = "master")
        val chunk2Meta = ChunkMetadata(validFrom = 5000, validTo = Long.MAX_VALUE, sequenceNumber = 2, branchName = "master")
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk1Dir, chunk1Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk2Dir, chunk2Meta).createChunkLockFile()

        assertThrows<IllegalStateException> {
            BranchChunkManager.create(this.testDir, "master")
        }
    }

    @Test
    fun chunkForTimestampForwardsToPreviousChunkIfChunkSequenceHasHoles(){
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk2Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 2)
        val chunk0Meta = ChunkMetadata(validFrom = 0, validTo = 1000, sequenceNumber = 0, branchName = "master")
        val chunk2Meta = ChunkMetadata(validFrom = 5000, validTo = Long.MAX_VALUE, sequenceNumber = 2, branchName = "master")
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        ChronoChunk.createNewChunk(chunk2Dir, chunk2Meta).createChunkLockFile()

        val bcm = BranchChunkManager.create(this.testDir, "master")
        val chunk = bcm.getChunkForTimestamp(1500)
        chunk should notBeNull()
        chunk!!.sequenceNumber shouldBe 0
    }

    @Test
    fun canPerformRollover(){
        val bcm = BranchChunkManager.create(this.testDir, "master")
        bcm.performRollover(1000){ processInfo ->
            processInfo.oldHeadChunk.sequenceNumber shouldBe 0
            processInfo.oldHeadChunk.validPeriod shouldBe Period.createRange(0, 1000)
            processInfo.newHeadChunk.sequenceNumber shouldBe 1
            processInfo.newHeadChunk.validPeriod shouldBe Period.createRange(1000, Long.MAX_VALUE)
        }
        bcm.headChunk.sequenceNumber shouldBe 1

        val chunk0 = ChronoChunk.readExistingChunk(File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0))
        val chunk1 = ChronoChunk.readExistingChunk(File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1))
        chunk0.sequenceNumber shouldBe 0
        chunk0.validPeriod shouldBe Period.createRange(0, 1000)
        chunk0.branchName shouldBe "master"
        chunk0.isHeadChunk shouldBe false
        chunk0.lockFile.exists() shouldBe true
        chunk1.sequenceNumber shouldBe 1
        chunk1.validPeriod shouldBe Period.createOpenEndedRange(1000)
        chunk1.branchName shouldBe "master"
        chunk1.isHeadChunk shouldBe true
        chunk1.lockFile.exists() shouldBe true
    }

    @Test
    fun resetsLastChunkToHeadOnStartup(){
        val chunk0Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 0)
        val chunk1Dir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DIRECTORY_PREFIX + 1)
        val chunk0Meta = ChunkMetadata(validFrom = 0, validTo = 1000, sequenceNumber = 0, branchName = "master")
        val chunk1Meta = ChunkMetadata(validFrom = 1000, validTo = 5000, sequenceNumber = 1, branchName = "master")
        // note that we do not have a "head" chunk with unlimited "validTo" here.
        ChronoChunk.createNewChunk(chunk0Dir, chunk0Meta).createChunkLockFile()
        val chunk1 = ChronoChunk.createNewChunk(chunk1Dir, chunk1Meta)
        chunk1.createChunkLockFile()

        val bcm = BranchChunkManager.create(this.testDir, "master")

        // now that the bcm is open, assert that we have a head chunk
        bcm.headChunk.sequenceNumber shouldBe 1
        bcm.headChunk.validPeriod shouldBe Period.createOpenEndedRange(1000)
    }
}