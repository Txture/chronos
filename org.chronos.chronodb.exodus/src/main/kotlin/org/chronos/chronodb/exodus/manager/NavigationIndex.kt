package org.chronos.chronodb.exodus.manager

import com.google.common.collect.Maps
import com.google.common.collect.Sets
import io.github.oshai.kotlinlogging.KotlinLogging
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException
import org.chronos.chronodb.exodus.kotlin.ext.parseAsString
import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.impl.engines.base.KeyspaceMetadata
import org.chronos.common.serialization.KryoManager

object NavigationIndex {

    private const val STORE_NAME = ChronoDBStoreLayout.STORE_NAME__NAVIGATION

    private val log = KotlinLogging.logger {}

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    /**
     * Performs an insertion into the Navigation Map.
     *
     * @param tx           The transaction to work on. Must not be `null`, must be open.
     * @param branchName   The name of the branch to insert. Must not be `null`.
     * @param keyspaceName The name of the keyspace to insert. Must not be `null`.
     * @param matrixName   The name of the matrix to insert. Must not be `null`.
     * @param timestamp    The timestamp at which to create the matrix. Must not be negative.
     */
    fun insert(tx: ExodusTransaction, branchName: String, keyspaceName: String, matrixName: String, timestamp: Long): KeyspaceMetadata {
        requireNonNegative(timestamp, "timestamp")
        val keyspaceToMatrixBinary = tx.get(STORE_NAME, branchName)
        val keyspaceToMetadata: MutableMap<String, KeyspaceMetadata>
        if (keyspaceToMatrixBinary == null) {
            // create a new map
            keyspaceToMetadata = Maps.newHashMap()
        } else {
            keyspaceToMetadata = KryoManager.deserialize(keyspaceToMatrixBinary.toByteArray())
        }
        val metadata = KeyspaceMetadata(keyspaceName, matrixName, timestamp)
        keyspaceToMetadata[keyspaceName] = metadata
        tx.put(STORE_NAME, branchName, KryoManager.serialize(keyspaceToMetadata).toByteIterable())
        log.trace {
            "Inserting into NavigationMap. Branch = '$branchName', keypsace = '$keyspaceName', matrix = '$matrixName'"
        }
        return metadata
    }

    /**
     * Checks if the branch with the given name exists in the Navigation Map.
     *
     * @param tx         The transaction to work on. Must not be `null`, must be open.
     * @param branchName The name of the branch to check existence for. Must not be `null`.
     * @return `true` if there exists a branch with the given name, otherwise `false`.
     */
    fun existsBranch(tx: ExodusTransaction, branchName: String): Boolean {
        log.trace { "Checking branch existence for branch '$branchName'" }
        val keyspaceToMetadata = tx.get(STORE_NAME, branchName)
        return keyspaceToMetadata != null && keyspaceToMetadata.length > 0
    }

    /**
     * Returns the names of all branches in the Navigation Map.
     *
     * @param tx The transaction to work on. Must not be `null`, must be open.
     * @return An immutable set of branch names. May be empty, but never `null`.
     */
    fun branchNames(tx: ExodusTransaction): Set<String> {
        log.trace { "Retrieving branch names" }
        val resultSet = Sets.newHashSet<String>()
        tx.openCursorOn(STORE_NAME).use { cursor ->
            while (cursor.next) {
                resultSet.add(cursor.key.parseAsString())
            }
        }
        return resultSet
    }

    /**
     * Deletes the branch with the given name from the Navigation Map.
     *
     * @param tx         The transaction to work on. Must not be `null`, must be open.
     * @param branchName The name of the branch to delete. Must not be `null`. Must refer to an existing branch.
     */
    fun deleteBranch(tx: ExodusTransaction, branchName: String) {
        assertBranchExists(tx, branchName)
        log.trace { "Deleting branch '$branchName' in navigation map" }
        tx.delete(STORE_NAME, branchName)
    }

    /**
     * Returns the map from keyspace name to matrix map name for the given branch.
     *
     * @param tx         The transaction to work on. Must not be `null`, must be open.
     * @param branchName The name of the branch to get the keyspace-to-matrix-name map for. Must not be `null`, must
     * refer to an existing branch.
     * @return The metadata for all known keyspaces in the given branch.
     * @throws ChronoDBBranchingException Thrown if there is no branch with the given name.
     */
    fun getKeyspaceMetadata(tx: ExodusTransaction, branchName: String): Set<KeyspaceMetadata> {
        assertBranchExists(tx, branchName)
        val keyspaceToMetadata = tx.get(STORE_NAME, branchName)?.toByteArray() ?: return Sets.newHashSet()
        val map = KryoManager.deserialize<Map<String, KeyspaceMetadata>>(keyspaceToMetadata)
        return Sets.newHashSet(map.values)
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    /**
     * Asserts that a branch with the given name exists.
     *
     *
     *
     * If there is a branch with the given name, then this method does nothing and returns immediately. Otherwise, a
     * [ChronoDBBranchingException] is thrown.
     *
     * @param tx         The transaction to work on. Must not be `null`, must be open.
     * @param branchName The name of the branch to check. Must not be `null`.
     * @throws ChronoDBBranchingException Thrown if there is no branch with the given name.
     */
    private fun assertBranchExists(tx: ExodusTransaction, branchName: String) {
        if (!existsBranch(tx, branchName)) {
            throw ChronoDBBranchingException("There is no branch named '$branchName'!")
        }
    }
}
