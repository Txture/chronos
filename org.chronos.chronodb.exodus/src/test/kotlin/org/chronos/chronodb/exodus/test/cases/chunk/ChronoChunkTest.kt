package org.chronos.chronodb.exodus.test.cases.chunk

import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.manager.chunk.ChunkMetadata
import org.chronos.chronodb.exodus.test.base.TestWithTempDir
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.lang.IllegalStateException

class ChronoChunkTest : TestWithTempDir() {

    @Test
    fun canCreateChunk(){
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 1
        )
        val chunk = ChronoChunk.createNewChunk(this.testDir, metadata)
        // this should have created the appropriate folder structure on disk...
        val dataDir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_DATA_DIRECTORY)
        dataDir.exists() shouldBe true
        val metaDir = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_METADATA_DIRECTORY)
        metaDir.exists() shouldBe true
        val chunkInfo = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_INFO_PROPERTIES)
        chunkInfo.exists() shouldBe true
        // however, it should not have created the chunk lock file yet
        val lockFile = File(this.testDir, ChronoDBDirectoryLayout.CHUNK_LOCK_FILE)
        lockFile.exists() shouldBe false

        chunk.createChunkLockFile()
        lockFile.exists() shouldBe true
    }

    @Test
    fun canReadChunk(){
        // first, create a chunk on disk
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 1
        )
        val chunk = ChronoChunk.createNewChunk(this.testDir, metadata)
        chunk.createChunkLockFile()
        // then, try to load it
        val reloadedChunk = ChronoChunk.readExistingChunk(this.testDir)
        reloadedChunk.branchName shouldBe metadata.branchName
        reloadedChunk.validPeriod shouldBe metadata.validPeriod
        reloadedChunk.sequenceNumber shouldBe metadata.sequenceNumber
    }

    @Test
    fun cannotLoadChunkWithoutLockFile(){
        // first, create a chunk on disk
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 1
        )
        val chunk = ChronoChunk.createNewChunk(this.testDir, metadata)

        assertThrows<IllegalStateException> {
            ChronoChunk.readExistingChunk(this.testDir)
        }
    }

    @Test
    fun canSetValidToTimestamp(){
        // first, create a chunk on disk
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 1
        )
        val chunk = ChronoChunk.createNewChunk(this.testDir, metadata)
        chunk.isHeadChunk shouldBe false

        chunk.setValidToTimestampTo(Long.MAX_VALUE)
        chunk.validPeriod.isOpenEnded shouldBe true
        chunk.validPeriod.upperBound shouldBe Long.MAX_VALUE
        chunk.isHeadChunk shouldBe true
    }

}