package org.chronos.chronodb.exodus.manager

import org.chronos.chronodb.api.MaintenanceManager
import org.chronos.chronodb.exodus.ExodusChronoDB
import java.lang.IllegalArgumentException
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Predicate
import kotlin.concurrent.withLock

class ExodusMaintenanceManager : MaintenanceManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private val owningDB: ExodusChronoDB
    private val rolloverLock: Lock

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    constructor(owningDB: ExodusChronoDB){
        this.owningDB = owningDB
        this.rolloverLock = ReentrantLock(true)
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun performRolloverOnBranch(branchName: String, updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.rolloverLock.withLock {
            this.owningDB.lockExclusive().use {
                val branch = this.owningDB.branchManager.getBranch(branchName)
                if(branch == null){
                    throw IllegalArgumentException("There is no branch named '${branchName}', cannot perform rollover!")
                }
                val tkvs = branch.temporalKeyValueStore as ExodusTkvs
                tkvs.performRollover(updateIndices)
            }
        }
    }

    override fun performRolloverOnAllBranches(updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.rolloverLock.withLock {
            this.owningDB.lockExclusive().use {
                // note: JavaDoc states explicitly that this method does not require ACID safety,
                // so it's ok to roll over the branches one by one.
                for (branchName in this.owningDB.branchManager.branchNames) {
                    this.performRolloverOnBranch(branchName, updateIndices)
                }
            }
        }
    }

    override fun performRolloverOnAllBranchesWhere(branchPredicate: Predicate<String>, updateIndices: Boolean) {
        this.owningDB.configuration.assertNotReadOnly()
        this.rolloverLock.withLock {
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

}