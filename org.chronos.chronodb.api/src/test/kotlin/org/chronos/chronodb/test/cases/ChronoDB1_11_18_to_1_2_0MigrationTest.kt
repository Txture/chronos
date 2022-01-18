package org.chronos.chronodb.test.cases

import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.util.ChronosFileUtils
import org.chronos.chronodb.test.cases.util.model.person.FirstNameIndexer
import org.chronos.chronodb.test.cases.util.model.person.LastNameIndexer
import org.chronos.common.test.ChronosUnitTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.chronos.common.util.ClasspathUtils
import org.hamcrest.Matchers
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.experimental.categories.Category
import strikt.api.expectThat
import strikt.assertions.*
import java.io.File

@Category(IntegrationTest::class)
class ChronoDB1_11_18_to_1_2_0MigrationTest : ChronosUnitTest() {

    /**
     * Logical Contents of this file:
     *
     * BRANCH: master
     * -------------------------------------
     * Persons:
     * - p1: John Doe
     * - p2: Jane Smith
     * - p3: Jack Smith
     * Animals:
     *
     * BRANCH: b1  (branched after last commit on 'master')
     * -------------------------------------
     * Persons:
     * - p1: John Doe
     * - p2: Jane Smith
     * - p3: Jack Smith
     * Animals:
     *
     * BRANCH: b2  (branched after last commit on 'b1')
     * -------------------------------------
     * Persons:
     * - p1: John Doe
     * - p2: Jane Smith
     * - p3: Jack Smith
     * - p4: Sarah Doe
     * Animals:
     * - a1: Rex
     *
     * INDICES:
     * - 'lastName' of type [LastNameIndexer]
     * - 'firstName' of type [FirstNameIndexer]
     */
    val testFileName = "migrationTestResources/ChronoDB_v1_11_18_MigrationTest_Exodus.zip"


    @Test
    fun canMigrate1_11_18_to_1_2_0() {
        // load up the resource file
        val testResourceZipFile = ClasspathUtils.getResourceAsFile(testFileName)
        Assert.assertNotNull(testResourceZipFile)
        Assert.assertTrue(testResourceZipFile!!.exists())
        // unzip the file to our test directory
        val testDir = this.testDirectory
        // unpack the *.zip file
        ChronosFileUtils.extractZipFile(testResourceZipFile, testDir)

        val graphDir = File(testDir, "chronodb_1_1_18_migration_baseline")
        Assert.assertTrue(graphDir.exists())
        Assert.assertTrue(graphDir.isDirectory)

        // open the ChronoDB instance
        val chronoDB = ChronoDB.FACTORY.create().database(ExodusChronoDB.BUILDER).onFile(graphDir).build() as ExodusChronoDB

        // at this point, the migration has already happened. Let's check the DB content
        chronoDB.use { db ->
            val branchNames = db.branchManager.branchNames
            assertEquals(setOf("master", "b1", "b2"), branchNames)

            val b1 = db.branchManager.getBranch("b1")
            val b2 = db.branchManager.getBranch("b2")

            // we should have an index on each branch
            val allIndices = db.indexManager.getIndices()
            val indicesByBranch = allIndices.groupBy { it.branch }
            val indicesById = allIndices.associateBy { it.id }
            assertEquals(setOf("master", "b1", "b2"), indicesByBranch.keys)

            val masterIndices = indicesByBranch.getValue("master")
            expectThat(masterIndices){
                one {
                    get { this.name }.isEqualTo("firstName")
                    get { this.parentIndexId }.isNull()
                    get { this.indexer }.isA<FirstNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.eternal())
                    get { this.branch }.isEqualTo("master")
                }
                one {
                    get { this.name }.isEqualTo("lastName")
                    get { this.parentIndexId }.isNull()
                    get { this.indexer }.isA<LastNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.eternal())
                    get { this.branch }.isEqualTo("master")
                }
            }

            val b1Indices = indicesByBranch.getValue("b1")
            expectThat(b1Indices){
                one {
                    get { this.name }.isEqualTo("firstName")
                    get { this.parentIndexId }.isNotNull().and {
                        get { indicesById[this] }.isNotNull().and {
                            get { this.name }.isEqualTo("firstName")
                            get { this.branch }.isEqualTo("master")
                        }
                    }
                    get { this.indexer }.isA<FirstNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.createOpenEndedRange(b1.branchingTimestamp))
                    get { this.branch }.isEqualTo("b1")
                }
                one {
                    get { this.name }.isEqualTo("lastName")
                    get { this.parentIndexId }.isNotNull().and {
                        get { indicesById[this] }.isNotNull().and {
                            get { this.name }.isEqualTo("lastName")
                            get { this.branch }.isEqualTo("master")
                        }
                    }
                    get { this.indexer }.isA<LastNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.createOpenEndedRange(b1.branchingTimestamp))
                    get { this.branch }.isEqualTo("b1")
                }
            }

            val b2Indices = indicesByBranch.getValue("b2")
            expectThat(b2Indices){
                one {
                    get { this.name }.isEqualTo("firstName")
                    get { this.parentIndexId }.isNotNull().and {
                        get { indicesById[this] }.isNotNull().and {
                            get { this.name }.isEqualTo("firstName")
                            get { this.branch }.isEqualTo("b1")
                        }
                    }
                    get { this.indexer }.isA<FirstNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.createOpenEndedRange(b2.branchingTimestamp))
                    get { this.branch }.isEqualTo("b2")
                }
                one {
                    get { this.name }.isEqualTo("lastName")
                    get { this.parentIndexId }.isNotNull().and {
                        get { indicesById[this] }.isNotNull().and {
                            get { this.name }.isEqualTo("lastName")
                            get { this.branch }.isEqualTo("b1")
                        }
                    }
                    get { this.indexer }.isA<LastNameIndexer>()
                    get { this.dirty }.isTrue()
                    get { this.validPeriod }.isEqualTo(Period.createOpenEndedRange(b2.branchingTimestamp))
                    get { this.branch }.isEqualTo("b2")
                }
            }

            // reindex and run some queries
            chronoDB.indexManager.reindexAll()

            db.tx().also { tx ->
                val result1 = tx.find()
                    .inKeyspace("persons")
                    .where("lastName").isEqualTo("Doe")
                    .or().where("lastName").isEqualTo("Smith")
                    .unqualifiedKeysAsSet
                expectThat(result1).containsExactlyInAnyOrder("p1", "p2", "p3")
            }

            db.tx("b1").also { tx ->
                val result1 = tx.find()
                    .inKeyspace("persons")
                    .where("lastName").isEqualTo("Doe")
                    .or().where("lastName").isEqualTo("Smith")
                    .unqualifiedKeysAsSet
                expectThat(result1).containsExactlyInAnyOrder("p1", "p2", "p3")
            }

            db.tx("b2").also { tx ->
                val result1 = tx.find()
                    .inKeyspace("persons")
                    .where("lastName").isEqualTo("Doe")
                    .or().where("lastName").isEqualTo("Smith")
                    .unqualifiedKeysAsSet
                expectThat(result1).containsExactlyInAnyOrder("p1", "p2", "p3", "p4")
            }
        }
    }

}