package org.chronos.chronodb.exodus.test.cases.chunk

import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.StoreConfig
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.manager.chunk.*
import org.chronos.chronodb.exodus.test.base.TestWithTempDir
import org.chronos.common.testing.kotlin.ext.notBeNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.io.FileReader
import java.nio.file.Files
import java.util.*

class ChunkMetadataTest : TestWithTempDir() {

    @Test
    fun canCreateMetadata(){
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 0
        )
        metadata should notBeNull()
    }

    @Test
    fun canWriteMetadata(){
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 0
        )
        metadata.writeBinaryTo(this.testDir)
        // check that the files have been created successfully
        Environments.newInstance(this.testDir).use { env ->
            env.computeInReadonlyTransaction { tx ->
                val store = env.openStore("metadata", StoreConfig.WITHOUT_DUPLICATES, tx)
                val validFromBytes = store.get(tx, PROPERTY_VALID_FROM)
                val validToBytes = store.get(tx, PROPERTY_VALID_TO)
                val branchNameBytes = store.get(tx, PROPERTY_BRANCH_NAME)
                val sequenceNumberBytes = store.get(tx, PROPERTY_SEQUENCE_NUMBER)

                validFromBytes should notBeNull()
                validToBytes should notBeNull()
                branchNameBytes should notBeNull()
                sequenceNumberBytes should notBeNull()

                validFromBytes!!.parseAsLong() shouldBe 0
                validToBytes!!.parseAsLong() shouldBe 1000
                branchNameBytes!!.parseAsString() shouldBe "master"
                sequenceNumberBytes!!.parseAsLong() shouldBe 0
            }
        }
    }

    @Test
    fun canReadMetadata(){
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 0
        )
        metadata.writeBinaryTo(this.testDir)
        // attempt to read it back in
        val readMetadata = ChunkMetadata.readFrom(this.testDir)
        // the objects should be equal before and after loading
        readMetadata shouldBe metadata
    }

    @Test
    fun failsOnMissingProperty(){
        Environments.newInstance(this.testDir).use { env ->
            env.executeInExclusiveTransaction { tx ->
                val store = env.openStore("metadata", StoreConfig.WITHOUT_DUPLICATES, tx)
                store.put(tx, PROPERTY_VALID_FROM, 0L.toByteIterable())
                store.put(tx, PROPERTY_VALID_TO, 1000L.toByteIterable())
                // ... we intentionally "forget" the branch name here...
                store.put(tx, PROPERTY_SEQUENCE_NUMBER, 0L.toByteIterable())
                tx.commit()
            }
        }
        // attempt to read it
        assertThrows<IllegalStateException> {
            ChunkMetadata.readFrom(this.testDir)
        }
    }

    @Test
    fun canWritePropertiesAsPlainText(){
        val metadata = ChunkMetadata(
                validFrom = 0,
                validTo = 1000,
                branchName = "master",
                sequenceNumber = 0
        )
        val propertiesFile = File(this.testDir, "test.properties")
        Files.createFile(propertiesFile.toPath())
        metadata.writePlainTextTo(propertiesFile)
        val allFiles = this.testDir.listFiles().toList()
        allFiles.size shouldBe 1
        allFiles.onlyElement().name shouldBe "test.properties"

        propertiesFile.exists() shouldBe true
        val properties = Properties()
        FileReader(propertiesFile).use { reader ->
            properties.load(reader)
        }
        properties.size shouldBe 4
        properties.getProperty(PROPERTY_VALID_FROM) shouldBe "0"
        properties.getProperty(PROPERTY_VALID_TO) shouldBe "1000"
        properties.getProperty(PROPERTY_BRANCH_NAME) shouldBe "master"
        properties.getProperty(PROPERTY_SEQUENCE_NUMBER) shouldBe "0"
    }

}