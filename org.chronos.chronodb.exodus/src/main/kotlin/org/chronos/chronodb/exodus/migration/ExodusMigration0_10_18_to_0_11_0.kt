package org.chronos.chronodb.exodus.migration

import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.TemporalExodusMatrix
import org.chronos.chronodb.exodus.kotlin.ext.parseAsInverseUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.manager.NavigationIndex
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.BranchInternal
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.migration.ChronosMigration
import org.chronos.chronodb.internal.api.migration.annotations.Migration
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata

@Migration(from = "0.10.18", to = "0.11.0")
class ExodusMigration0_10_18_to_0_11_0 : ChronosMigration<ExodusChronoDB> {

    override fun execute(chronoDB: ExodusChronoDB) {
        // this migration updates the keyspace creation timestamps which may have gotten out-of-sync.
        val branchNameToKeyspaceMetadata = getBranchNameToKeyspaceMetadata(chronoDB)
        for ((branch, metadataSet) in branchNameToKeyspaceMetadata) {
            for (metadata in metadataSet) {
                val earliestCommit = findEarliestCommitInKeyspace(chronoDB, branch, metadata)
                // note: creation timestamps can only become SMALLER by this operation. If
                // a creation timestamp is earlier than the first actual entry (this could
                // happen e.g. due to dateback deletions), that's fine. We only have to
                // prevent the case where an entry exists e.g. at t=100, but the creation
                // timestamp of the keyspace is later, e.g. at t=120.
                if (earliestCommit > 0 && earliestCommit < metadata.creationTimestamp) {
                    // we found an out-of-sync creation timestamp, update it
                    updateEarliestCommitTimestamp(chronoDB, branch, metadata, earliestCommit)
                }
            }
        }
    }

    private fun getBranchNameToKeyspaceMetadata(chronoDB: ExodusChronoDB): Map<Branch, Set<KeyspaceMetadata>> {
        val branches = chronoDB.branchManager.branchNames.asSequence().map { name -> chronoDB.branchManager.getBranch(name) }.toList()
        return chronoDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { globalTx ->
            branches.asSequence().map { branch ->
                branch to NavigationIndex.getKeyspaceMetadata(globalTx, branch.name)
            }.toMap()
        }
    }

    private fun findEarliestCommitInKeyspace(chronoDB: ExodusChronoDB, branch: Branch, metadata: KeyspaceMetadata): Long {
        val globalChunkManager = chronoDB.globalChunkManager
        val bcm = globalChunkManager.getOrCreateChunkManagerForBranch(branch)
        for (chunk in bcm.getChunksForPeriod(Period.eternal())) {
            val timestampInChunk = globalChunkManager.openReadOnlyTransactionOn(chunk).use { tx ->
                val storeName = metadata.matrixTableName + TemporalExodusMatrix.INVERSE_STORE_NAME_SUFFIX
                earliestCommitTimestampInInverseStore(tx, storeName)
            }
            // did we find an entry in this keyspace in this chunk?
            if (timestampInChunk > 0) {
                // chunks are sorted in ascending order, so as soon as we find a timestamp in this
                // keyspace, it's guaranteed that all later chunks will only have HIGHER timestamps,
                // which is why we can break out of the loop here.
                return timestampInChunk
            } else {
                // we found no entry in this keyspace in this chunk, try the next chunk
                continue
            }
        }
        // we didn't find *any* entries in this keyspace, at all...?
        return -1
    }

    private fun earliestCommitTimestampInInverseStore(tx: ExodusTransaction, storeName: String): Long {
        if (!tx.storeExists(storeName)) {
            return -1
        } else {
            tx.openCursorOn(storeName).use { cursor ->
                if (cursor.next) {
                    // store is non-empty, take the first entry
                    val iutk = cursor.key.parseAsInverseUnqualifiedTemporalKey()
                    return iutk.timestamp
                } else {
                    // store is empty, no entry to report
                    return -1
                }
            }
        }
    }

    private fun updateEarliestCommitTimestamp(chronoDB: ExodusChronoDB, branch: Branch, metadata: KeyspaceMetadata, earliestCommit: Long) {
        require(earliestCommit >= 0) { "Precondition violation - argument 'earliestCommit' must not be negative!" }
        require(earliestCommit < metadata.creationTimestamp) { "Precondition violation - argument 'earliestCommit' (value: ${earliestCommit}) must be less than the current keyspace creation timestamp ${metadata.creationTimestamp}!" }
        val branchInternal = branch as BranchInternal
        branchInternal.temporalKeyValueStore.updateCreationTimestampForKeyspace(metadata.keyspaceName, earliestCommit)
    }

}