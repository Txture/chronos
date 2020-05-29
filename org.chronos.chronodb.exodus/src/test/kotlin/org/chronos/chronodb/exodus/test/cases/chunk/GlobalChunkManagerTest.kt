package org.chronos.chronodb.exodus.test.cases.chunk

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.environment.EnvironmentManager
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.exodus.test.base.TestWithTempDir
import org.chronos.common.testing.kotlin.ext.notBeNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.hamcrest.Matchers.sameInstance
import org.junit.jupiter.api.Test
import java.io.File

class GlobalChunkManagerTest : TestWithTempDir() {

    @Test
    fun canCreateGlobalChunkManager() {
        withGlobalChunkManager { gcm ->
            gcm should notBeNull()
        }
    }

    @Test
    fun globalChunkManagerAutomaticallyCreatesGlobalEnvAndMasterBranch() {
        withGlobalChunkManager {
            val globalDir = File(this.testDir, ChronoDBDirectoryLayout.GLOBAL_DIRECTORY)
            globalDir.exists() shouldBe true
            globalDir.isDirectory shouldBe true

            val branchesDir = File(this.testDir, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY)
            branchesDir.exists() shouldBe true
            branchesDir.isDirectory shouldBe true

            val masterBranchDir = File(branchesDir, ChronoDBDirectoryLayout.MASTER_BRANCH_DIRECTORY)
            masterBranchDir.exists() shouldBe true
            masterBranchDir.isDirectory shouldBe true
        }
    }

    @Test
    fun canGetChunkManagerForBranch() {
        withGlobalChunkManager { gcm ->
            gcm.hasChunkManagerForBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER) shouldBe true
            val masterBCM = gcm.getChunkManagerForBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)
            masterBCM should notBeNull()
            masterBCM.branchName shouldBe ChronoDBConstants.MASTER_BRANCH_IDENTIFIER

            // let's create a branch chunk manager for a new branch
            gcm.hasChunkManagerForBranch("test") shouldBe false
            val branchDirName = ChronoDBDirectoryLayout.BRANCH_DIRECTORY_PREFIX + "test"
            val testBranchMetadata = IBranchMetadata.create("test", ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, 1234L, branchDirName)
            val testBranchBCM = gcm.getOrCreateChunkManagerForBranch(testBranchMetadata)
            testBranchBCM should notBeNull()
            testBranchBCM.branchName shouldBe "test"
            testBranchBCM.branchDirectory shouldBe File(File(this.testDir, ChronoDBDirectoryLayout.BRANCHES_DIRECTORY), branchDirName)

            // make sure that we receive the same instance of the branch chunk manager
            gcm.hasChunkManagerForBranch("test") shouldBe true
            gcm.getChunkManagerForBranch("test") shouldBe sameInstance(testBranchBCM)
        }
    }

    @Test
    fun canCreateTransactionOnGlobalEnvironment() {
        withGlobalChunkManager { gcm ->
            gcm.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
                tx.put("test", "Hello", "World".toByteIterable())
                tx.put("test", "Foo", "Bar".toByteIterable())
                tx.commit()
            }
            gcm.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
                tx.get("test", "Hello") shouldBe "World".toByteIterable()
                tx.get("test", "Foo") shouldBe "Bar".toByteIterable()
            }
        }
    }

    @Test
    fun canCreateTransactionOnBranchHeadChunk() {
        val master = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
        withGlobalChunkManager { gcm ->
            gcm.openReadWriteTransactionOnHeadChunkOf(master).use { tx ->
                tx.put("test", "Hello", "World".toByteIterable())
                tx.put("test", "Foo", "Bar".toByteIterable())
                tx.commit()
            }
            gcm.openReadOnlyTransactionOnHeadChunkOf(master).use{ tx ->
                tx.get("test", "Hello") shouldBe "World".toByteIterable()
                tx.get("test", "Foo") shouldBe "Bar".toByteIterable()
            }
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun withEnvironmentManager(consumer: (EnvironmentManager) -> Unit) {
        EnvironmentManager(emptyMap(), 20, 60).use { envManager ->
            consumer(envManager)
        }
    }

    private fun withGlobalChunkManager(consumer: (GlobalChunkManager) -> Unit) {
        withEnvironmentManager { envManager ->
            val branchDirToBranchName = { dir: File -> dir.name.substring(ChronoDBDirectoryLayout.BRANCH_DIRECTORY_PREFIX.length) }
            GlobalChunkManager.create(this.testDir, branchDirToBranchName, envManager).use { gcm ->
                consumer(gcm)
                return@use gcm
            }.let{ it.isClosed shouldBe true}
        }
    }
}