package org.chronos.chronodb.exodus.manager

import com.google.common.collect.Maps
import mu.KotlinLogging
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.cast
import org.chronos.chronodb.exodus.kotlin.ext.mapTo
import org.chronos.chronodb.exodus.layout.ChronoDBDirectoryLayout
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.BranchInternal
import org.chronos.chronodb.internal.api.ChronoDBInternal
import org.chronos.chronodb.internal.impl.BranchImpl
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.chronos.chronodb.internal.impl.MatrixUtils
import org.chronos.chronodb.internal.impl.engines.base.AbstractBranchManager
import java.util.*

class ExodusBranchManager : AbstractBranchManager {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private val owningDB: ExodusChronoDB
    private val loadedBranches = Maps.newConcurrentMap<String, BranchInternal>()
    private val branchMetadata = Maps.newConcurrentMap<String, IBranchMetadata>()

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB) : super(::createDirectoryNameForBranchName) {
        this.owningDB = owningDB
        this.loadBranchMetadata()
        this.ensureMasterBranchExists()
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun getBranchNames(): Set<String> {
        return this.branchMetadata.values.asSequence().map(IBranchMetadata::getName).toSet()
    }

    override fun createBranch(metadata: IBranchMetadata): BranchInternal {
        this.owningDB.configuration.assertNotReadOnly()
        return this.createBranchInternal(metadata)
    }

    override fun getBranchInternal(name: String?): BranchInternal? {
        var branch: BranchInternal? = this.loadedBranches[name]
        if (branch != null) {
            // already loaded
            return branch
        }
        // not loaded yet; load it
        val metadata = this.branchMetadata[name] ?: return null
        if (metadata.parentName == null) {
            // we are the master branch
            branch = BranchImpl.createMasterBranch()
        } else {
            val parentBranch = this.getBranch(metadata.parentName)
            branch = BranchImpl.createBranch(metadata, parentBranch)
        }
        // attach the TKVS to the branch
        this.attachTKVS(branch)
        this.loadedBranches[branch.name] = branch
        return branch
    }

    override fun deleteSingleBranch(branch: Branch) {
        this.loadedBranches.remove(branch.name)
        this.branchMetadata.remove(branch.name)
        this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            NavigationIndex.deleteBranch(tx, branch.name)
            BranchMetadataIndex.deleteBranch(tx, branch.name)
            tx.commit()
        }
        this.owningDB.datebackManager.deleteLogsForBranch(branch.name)
        this.owningDB.globalChunkManager.deleteBranch(branch)
    }

    override fun getOwningDB(): ChronoDBInternal {
        return this.owningDB
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun loadBranchMetadata() {
        this.openTxReadonly().use { tx ->
            BranchMetadataIndex.values(tx).asSequence()
                .map(this.owningDB.serializationManager::deserialize)
                .cast(IBranchMetadata::class)
                .mapTo(this.branchMetadata) { it.name to it }
        }
    }

    private fun ensureMasterBranchExists() {
        val masterBranchMetadata = IBranchMetadata.createMasterBranchMetadata()
        if (this.existsBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)) {
            // we know that the master branch exists in our navigation map.
            // ensure that it also exists in the branch metadata map
            this.openTxReadWrite().use { tx ->
                if (BranchMetadataIndex.getMetadata(tx, masterBranchMetadata.name) == null) {
                    // master branch metadata does not exist yet; insert it
                    BranchMetadataIndex.insertOrUpdate(
                        tx,
                        masterBranchMetadata.name,
                        this.owningDB.serializationManager.serialize(masterBranchMetadata)
                    )
                    tx.commit()
                }
            }
            return
        }
        this.createBranchInternal(masterBranchMetadata)
    }

    private fun openTxReadonly(): ExodusTransaction {
        return this.owningDB.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment()
    }

    private fun openTxReadWrite(): ExodusTransaction {
        return this.owningDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment()
    }

    private fun createBranchInternal(metadata: IBranchMetadata): BranchInternal {
        val parentBranch: BranchInternal? = metadata.parentName?.let(this::getBranch)
        val branch: BranchImpl = if (parentBranch == null) {
            // we have no parent branch -> create the master branch
            BranchImpl.createMasterBranch()
        } else {
            // create child branch for the parent
            BranchImpl.createBranch(metadata, parentBranch)
        }
        this.owningDB.globalChunkManager.getOrCreateChunkManagerForBranch(branch)
        this.openTxReadWrite().use { tx ->
            val keyspaceName = ChronoDBConstants.DEFAULT_KEYSPACE_NAME
            val tableName = MatrixUtils.generateRandomName()
            log.trace { "Creating branch: [" + branch.name + ", " + keyspaceName + ", " + tableName + "]" }
            NavigationIndex.insert(tx, branch.name, keyspaceName, tableName, 0L)
            BranchMetadataIndex.insertOrUpdate(tx, branch.metadata.name, this.owningDB.serializationManager.serialize(branch.metadata))
            tx.commit()
        }
        this.attachTKVS(branch)
        this.loadedBranches[branch.name] = branch
        this.branchMetadata[branch.name] = branch.metadata
        return branch
    }

    private fun attachTKVS(branch: BranchInternal): ExodusTkvs {
        return ExodusTkvs(this.owningDB, branch)
    }

}

private fun createDirectoryNameForBranchName(branchName: String): String {
    return ChronoDBDirectoryLayout.BRANCH_DIRECTORY_PREFIX + UUID.randomUUID().toString().replace('-', '_')
}
