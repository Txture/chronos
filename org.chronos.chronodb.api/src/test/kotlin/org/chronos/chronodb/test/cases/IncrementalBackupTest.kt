package org.chronos.chronodb.test.cases

import com.google.common.collect.Lists
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.exceptions.ChronoDBBackupException
import org.chronos.chronodb.internal.impl.dump.incremental.CibFileReader
import org.chronos.chronodb.internal.util.ChronosFileUtils
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.common.testing.kotlin.ext.beNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.common.version.ChronosVersion
import org.hamcrest.Matchers.*
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertThat
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit

class IncrementalBackupTest : AllChronoDBBackendsTest() {

    @Test
    fun canCreateIncrementalBackupFromZeroStartTime() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)

        val tx1 = db.tx()
        tx1.put("foo", "bar")
        val commit1 = tx1.commit("one")

        println("Inserted [foo->bar]@master at ${commit1}")

        db.branchManager.createBranch("b1")
        val tx2 = db.tx("b1")
        tx2.put("Hello", "World")
        val commit2 = tx2.commit("two")

        println("Inserted [Hello->World]@b1 at ${commit2}")

        db.branchManager.createBranch("b1", "b2")
        val tx3 = db.tx("b2")
        tx3.put("foo", "baz")
        val commit3 = tx3.commit("three")

        println("Inserted [foo->baz]@b2 at ${commit3}")

        println("Before Rollover: \n\t${db.branchManager.branches.map { "${it.name}: ${it.now}" }.joinToString(separator = "\n\t")}\n")
        db.maintenanceManager.performRolloverOnAllBranches()
        println("After Rollover: \n\t${db.branchManager.branches.map { "${it.name}: ${it.now}" }.joinToString(separator = "\n\t")}\n")

        val tx4 = db.tx()
        tx4.put("John", "Doe")
        val commit4 = tx4.commit()

        println("Inserted [John->Doe]@master at ${commit4}")

        val backupInfo = db.backupManager.createIncrementalBackup(0, 0)
        assertNotNull(backupInfo)
        try {
            assertNotNull(backupInfo.cibFile)
            assertNotNull(backupInfo.metadata)
            assertThat(backupInfo.metadata.chronosVersion, `is`(ChronosVersion.getCurrentVersion()))
            assertThat(backupInfo.metadata.previousRequestWallClockTime, `is`(0L))
            assertThat(backupInfo.metadata.requestStartTimestamp, `is`(0L))
            assertThat(backupInfo.metadata.now, `is`(db.tx().timestamp))
            assertThat(backupInfo.metadata.wallClockTime, `is`(greaterThanOrEqualTo(db.tx().timestamp)))

            val exportDir = File(this.testDirectory, "export")
            Files.createDirectory(exportDir.toPath())
            ChronosFileUtils.extractZipFile(backupInfo.cibFile, exportDir)

            exportDir.walkTopDown().forEach {
                println("${if (it.isFile) {
                    "[FIL]"
                } else {
                    "[DIR]"
                }} ${it.absolutePath} [${FileUtils.byteCountToDisplaySize(it.length())}]")
            }

        } finally {
            // delete the temp dir and all contents
            backupInfo.cibFile.parentFile.deleteRecursively()
        }
    }

    @Test
    fun canCreateIncrementalBackupFromNonZeroStartTime() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)

        val tx1 = db.tx()
        tx1.put("John", "Doe")
        tx1.put("Foo", "Bar")
        tx1.commit()

        val tx2 = db.tx()
        tx2.remove("Foo")
        tx2.commit()

        db.maintenanceManager.performRolloverOnAllBranches()

        val afterRollover = db.tx().timestamp

        val tx3 = db.tx()
        tx3.put("Pi", 3.1415)
        tx3.commit()

        val backup = db.backupManager.createIncrementalBackup(afterRollover, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
        try{
            assertThat(backup.metadata.now, `is`(db.tx().timestamp))
            assertThat(backup.metadata.requestStartTimestamp, `is`(afterRollover))

            CibFileReader(backup.cibFile).use { inspector ->
                inspector.incrementalBackupInfo.now shouldBe db.tx().timestamp
                inspector.incrementalBackupInfo.requestStartTimestamp shouldBe afterRollover

                val masterChunks = inspector.listContainedChunksForMasterBranch()
                // chunk 0 should not be contained due to our request timestamp!
                masterChunks should contains(1L)

                val chunkData = inspector.readChunkContents(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, 1)
                chunkData.size shouldBe 2
                val keys = chunkData.asSequence().map { it.chronoIdentifier.key }.toSet()
                keys shouldBe setOf("John", "Pi")
            }
        }finally{
            backup.cibFile.parentFile.deleteRecursively()
        }
    }

    @Test
    fun headChunkIsAlwaysIncludedInIncrementalBackup() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)
        val tx1 = db.tx()
        tx1.put("John", "Doe")
        tx1.put("Foo", "Bar")
        tx1.commit()

        val tx2 = db.tx()
        tx2.remove("Foo")
        tx2.commit()

        // create a backup starting from "now"
        // (this is a rather extreme case; regardless, the head chunk should be exported)
        val backup = db.backupManager.createIncrementalBackup(db.tx().timestamp, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
        try{
            backup.metadata.now shouldBe db.tx().timestamp
            backup.metadata.requestStartTimestamp shouldBe greaterThan(0L)

            CibFileReader(backup.cibFile).use { inspector ->
                inspector.incrementalBackupInfo.now shouldBe db.tx().timestamp
                inspector.incrementalBackupInfo.requestStartTimestamp shouldBe greaterThan(0L)

                val masterChunks = inspector.listContainedChunksForMasterBranch()
                // chunk 0 should be contained because its the head
                masterChunks should contains(0L)

                val chunkData = inspector.readChunkContents(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, 0)
                chunkData.size shouldBe 3
                val keys = chunkData.asSequence().map { it.chronoIdentifier.key }.toSet()
                keys shouldBe setOf("John", "Foo")
            }
        }finally{
            backup.cibFile.parentFile.deleteRecursively()
        }
    }

    @Test
    fun datebacksCauseReBackupOfOldChunks() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)

        val tx1 = db.tx()
        tx1.put("John", "Doe")
        tx1.put("Foo", "Bar")
        val firstCommit = tx1.commit()

        val tx2 = db.tx()
        tx2.remove("Foo")
        tx2.commit()

        db.maintenanceManager.performRolloverOnAllBranches()

        val afterRollover = db.tx().timestamp

        val tx3 = db.tx()
        tx3.put("Pi", 3.1415)
        tx3.commit()

        // dateback-inject a value
        db.datebackManager.datebackOnMaster { dateback ->
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", firstCommit, "World")
        }

        // note that we are performing the backup AFTER the rollover, but we should STILL get Chunk 0 because it is
        // affected by a dateback which happened within the last hour (last (faked) request was one hour ago)
        val backup = db.backupManager.createIncrementalBackup(afterRollover, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
        try{
            CibFileReader(backup.cibFile).use { inspector ->
                inspector.incrementalBackupInfo.now shouldBe db.tx().timestamp
                inspector.incrementalBackupInfo.requestStartTimestamp shouldBe greaterThan(0L)

                inspector.globalData.datebackLog.size shouldBe 1

                inspector.listContainedChunksForMasterBranch() should containsInAnyOrder(0L, 1L)
            }
        }finally{
            backup.cibFile.parentFile.deleteRecursively()
        }
    }


    @Test
    fun canReadSimpleBackup() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)
        val cibFile = this.getSrcTestResourcesFile("backup_test1.cib")
        assertThat("Test backup file not found!", cibFile.exists(), `is`(true))

        db.backupManager.loadIncrementalBackups(listOf(cibFile))

        assertThat(db.branchManager.branches.size, `is`(3))
        val branchMaster = db.branchManager.branches.find{ it.name == ChronoDBConstants.MASTER_BRANCH_IDENTIFIER }
        assertNotNull(branchMaster)
        val branchB1 = db.branchManager.branches.find { it.name == "b1" }
        assertNotNull(branchB1)
        val branchB2 = db.branchManager.branches.find { it.name == "b2" }
        assertThat(branchB2, `is`(notNullValue()))
        assertNotNull(branchB2)

        assertThat(branchB2!!.origin, `is`(branchB1))
        assertThat(branchB1!!.origin, `is`(branchMaster))
        assertThat(branchB2.branchingTimestamp, `is`(greaterThan(branchB1.branchingTimestamp)))
        assertThat(branchB1.branchingTimestamp, `is`(greaterThan(0L)))

        assertThat(db.tx().get("foo"), `is`("bar"))
        assertThat(db.tx().get("John"), `is`("Doe"))
        val commitsOnMaster = Lists.newArrayList(db.tx().getCommitMetadataBetween(0L, db.tx().timestamp))
        assertThat(commitsOnMaster.size, `is`(2))
        assertThat(commitsOnMaster[0].value, `is`(nullValue()))
        assertThat(commitsOnMaster[1].value as String, `is`("one"))

        assertThat(db.tx("b1").get("foo"), `is`("bar"))
        assertThat(db.tx("b1").get("Hello"), `is`("World"))
        assertThat(db.tx("b1").get("John"), `is`(nullValue()))

        assertThat(db.tx("b2").get("foo"), `is`("baz"))
        assertThat(db.tx("b2").get("Hello"), `is`("World"))
        assertThat(db.tx("b2").get("John"), `is`(nullValue()))

    }

    @Test
    fun canExportAndImportMultipleIncrementalBackups(){
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)

        val tx1 = db.tx()
        tx1.put("John", "Doe")
        tx1.put("Foo", "Bar")
        val commit1 = tx1.commit()

        val tx2 = db.tx()
        tx2.remove("Foo")
        val commit2 = tx2.commit()

        db.maintenanceManager.performRolloverOnAllBranches()

        val backup1 = db.backupManager.createIncrementalBackup(0L, 0L)
        try{
            val tx3 = db.tx()
            tx3.put("Pi", 3.1415)
            val now = tx3.commit()

            val backup2 = db.backupManager.createIncrementalBackup(backup1.metadata.now, backup1.metadata.requestStartTimestamp)
            try{
                val newDB = this.reinstantiateDB()
                newDB.backupManager.loadIncrementalBackups(listOf(backup1.cibFile, backup2.cibFile))

                newDB.tx().timestamp shouldBe now
                newDB.tx().get<Double>("Pi") shouldBe 3.1415
                newDB.tx().get<String>("John") shouldBe "Doe"
                newDB.tx().get<Any?>("Foo") should beNull()

                newDB.tx().getCommitTimestampsBetween(0, now).asSequence().toList() shouldBe listOf(now, commit2, commit1)
            }finally{
                backup2.cibFile.parentFile.deleteRecursively()
            }
        }finally{
            backup1.cibFile.parentFile.deleteRecursively()
        }
    }


    @Test(expected = ChronoDBBackupException::class)
    fun canNotReadSimpleBackupOfCorruptedBackup() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)
        val cibFile = this.getSrcTestResourcesFile("backup_test1_corrupted.cib")
        assertThat("Test backup file not found!", cibFile.exists(), `is`(true))

        db.backupManager.loadIncrementalBackups(listOf(cibFile))
    }

    @Test(expected = ChronoDBBackupException::class)
    fun canNotReadIncrementalBackupWithFileMissing() {
        val db = this.chronoDB
        assumeIncrementalBackupIsSupported(db)
        val cibFile = this.getSrcTestResourcesFile("chronos_backup_inc2.cib")
        assertThat("Test backup file not found!", cibFile.exists(), `is`(true))

        db.backupManager.loadIncrementalBackups(listOf(cibFile))
    }

}