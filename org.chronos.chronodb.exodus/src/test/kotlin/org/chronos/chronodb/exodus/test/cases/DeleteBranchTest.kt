package org.chronos.chronodb.exodus.test.cases

import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.common.test.ChronosUnitTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(IntegrationTest::class)
class DeleteBranchTest : ChronosUnitTest() {

    @Test
    fun canDeleteSingleBranchAndFiles() {
        ChronoDB.FACTORY.create()
            .database(ExodusChronoDB.BUILDER)
            .onFile(this.testDirectory)
            .build()
            .use { db ->
                db as ExodusChronoDB
                db.tx().also { tx ->
                    tx.put("hello", "world")
                    tx.commit()
                }
                val branch = db.branchManager.createBranch("sub")
                db.tx(branch.name).also { tx ->
                    tx.put("foo", "bar")
                    tx.commit()
                }

                val subBCM = db.globalChunkManager.getChunkManagerForBranch("sub")
                val subDir = subBCM.branchDirectory
                assertTrue(subDir.exists())
                assertTrue(subDir.isDirectory)
                assertFalse(FileUtils.isEmptyDirectory(subDir))

                db.branchManager.deleteBranchRecursively(branch.name)
                assertFalse(db.branchManager.existsBranch("sub"))
                assertFalse(subDir.exists())
            }
    }

    @Test
    fun canDeleteBranchAndFilesRecursively() {
        ChronoDB.FACTORY.create()
            .database(ExodusChronoDB.BUILDER)
            .onFile(this.testDirectory)
            .build()
            .use { db ->
                db as ExodusChronoDB
                db.tx().also { tx ->
                    tx.put("hello", "world")
                    tx.commit()
                }
                val branch = db.branchManager.createBranch("sub")
                db.tx(branch.name).also { tx ->
                    tx.put("foo", "bar")
                    tx.commit()
                }
                db.branchManager.createBranch("sub", "sub2")

                val subBCM = db.globalChunkManager.getChunkManagerForBranch("sub")
                val subDir = subBCM.branchDirectory
                assertTrue(subDir.exists())
                assertTrue(subDir.isDirectory)
                assertFalse(FileUtils.isEmptyDirectory(subDir))

                val sub2BCM = db.globalChunkManager.getChunkManagerForBranch("sub2")
                val sub2Dir = sub2BCM.branchDirectory
                assertTrue(sub2Dir.exists())
                assertTrue(sub2Dir.isDirectory)

                db.branchManager.deleteBranchRecursively(branch.name)

                assertFalse(db.branchManager.existsBranch("sub"))
                assertFalse(db.branchManager.existsBranch("sub2"))

                assertFalse(subDir.exists())
                assertFalse(sub2Dir.exists())
            }
    }


    @Test
    fun canDeleteChildBranch() {
        ChronoDB.FACTORY.create()
            .database(ExodusChronoDB.BUILDER)
            .onFile(this.testDirectory)
            .build()
            .use { db ->
                db as ExodusChronoDB
                db.tx().also { tx ->
                    tx.put("hello", "world")
                    tx.commit()
                }
                val branch = db.branchManager.createBranch("sub")
                db.tx(branch.name).also { tx ->
                    tx.put("foo", "bar")
                    tx.commit()
                }
                db.branchManager.createBranch("sub", "sub2")

                val subBCM = db.globalChunkManager.getChunkManagerForBranch("sub")
                val subDir = subBCM.branchDirectory
                assertTrue(subDir.exists())
                assertTrue(subDir.isDirectory)
                assertFalse(FileUtils.isEmptyDirectory(subDir))

                val sub2BCM = db.globalChunkManager.getChunkManagerForBranch("sub2")
                val sub2Dir = sub2BCM.branchDirectory
                assertTrue(sub2Dir.exists())
                assertTrue(sub2Dir.isDirectory)

                db.branchManager.deleteBranchRecursively("sub2")

                assertTrue(db.branchManager.existsBranch("sub"))
                assertFalse(db.branchManager.existsBranch("sub2"))

                assertTrue(subDir.exists())
                assertTrue(subDir.isDirectory)
                assertFalse(sub2Dir.exists())
            }
    }
}