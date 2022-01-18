package org.chronos.chronodb.test.cases

import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.internal.util.ChronosFileUtils
import org.chronos.common.test.ChronosUnitTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.chronos.common.util.ClasspathUtils
import org.hamcrest.Matchers.*
import org.junit.Assert.*
import org.junit.Test
import org.junit.experimental.categories.Category
import java.io.File

@Category(IntegrationTest::class)
class ChronoDB0_10_18_to_0_11_0MigrationTest : ChronosUnitTest() {

    val testFileName = "migrationTestResources/chronos_0_10_18.zip"

    @Test
    fun canMigrate0_10_18_to_0_11_0(){
        // load up the resource file
        val testResourceZipFile = ClasspathUtils.getResourceAsFile(testFileName)
        assertNotNull(testResourceZipFile)
        assertTrue(testResourceZipFile!!.exists())
        // unzip the file to our test directory
        val testDir = this.testDirectory
        // unpack the *.zip file
        ChronosFileUtils.extractZipFile(testResourceZipFile, testDir)

        val graphDir = File(testDir, "chronos_0_10_18_graph")
        assertTrue(graphDir.exists())
        assertTrue(graphDir.isDirectory)

        // open the ChronoDB instance
        val chronoDB = ChronoDB.FACTORY.create().database(ExodusChronoDB.BUILDER).onFile(graphDir).build() as ExodusChronoDB
        // at this point, the migration has already happened. Let's check the DB content
        chronoDB.use { db ->
            val branchNames = db.branchManager.branchNames
            assertThat(branchNames, containsInAnyOrder(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, "mybranch"))

            val keyspaces = db.tx().keyspaces()
            assertThat(keyspaces, hasItems("edge", "vertex"))

            // assert that all keyspaces have been created at (or before) the first commit.
            val keyspaceMetadata = db.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
                NavigationIndex.getKeyspaceMetadata(tx, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)
            }

            val commitTimestamps = db.tx().getCommitTimestampsAfter(0, 1000)
            val firstCommit = commitTimestamps.minOrNull() ?: -1
            assertThat(firstCommit, `is`(greaterThan(0L)))

            for(ksm in keyspaceMetadata){
                assertThat(ksm.creationTimestamp, `is`(lessThanOrEqualTo(firstCommit)))
            }
        }
    }

}