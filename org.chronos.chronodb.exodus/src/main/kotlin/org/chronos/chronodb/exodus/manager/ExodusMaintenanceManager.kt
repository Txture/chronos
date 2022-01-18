package org.chronos.chronodb.exodus.manager

import org.chronos.chronodb.api.MaintenanceManager
import org.chronos.chronodb.exodus.ExodusChronoDB
import java.util.function.Predicate

class ExodusMaintenanceManager : MaintenanceManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private val owningDB: ExodusChronoDB

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB) {
        this.owningDB = owningDB
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Synchronized
    override fun performRolloverOnBranch(branchName: String, updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.owningDB.lockExclusive().use {
            val branch = this.owningDB.branchManager.getBranch(branchName)
            if (branch == null) {
                throw IllegalArgumentException("There is no branch named '${branchName}', cannot perform rollover!")
            }
            val tkvs = branch.temporalKeyValueStore as ExodusTkvs
            tkvs.performRollover(updateIndices)
        }
    }

    @Synchronized
    override fun performRolloverOnAllBranches(updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.owningDB.lockExclusive().use {
            // note: JavaDoc states explicitly that this method does not require ACID safety,
            // so it's ok to roll over the branches one by one.
            for (branchName in this.owningDB.branchManager.branchNames) {
                this.performRolloverOnBranch(branchName, updateIndices)
            }
        }
    }

    @Synchronized
    override fun performRolloverOnAllBranchesWhere(branchPredicate: Predicate<String>, updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.owningDB.lockExclusive().use {
            // note: JavaDoc states explicitly that this method does not require ACID safety,
            // so it's ok to roll over the branches one by one.
            for (branchName in this.owningDB.branchManager.branchNames) {
                if (!branchPredicate.test(branchName)) {
                    // predicate says no...
                    continue
                }
                this.performRolloverOnBranch(branchName, updateIndices)
            }
        }
    }

}