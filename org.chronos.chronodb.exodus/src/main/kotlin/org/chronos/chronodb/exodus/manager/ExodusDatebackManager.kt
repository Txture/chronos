package org.chronos.chronodb.exodus.manager

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.internal.impl.dateback.AbstractDatebackManager
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation
import java.util.*

class ExodusDatebackManager(dbInstance: ExodusChronoDB) : AbstractDatebackManager(dbInstance) {

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    override fun writeDatebackOperationToLog(operation: DatebackOperation) {
        this.owningDb.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            tx.put(
                    ChronoDBStoreLayout.STORE_NAME__DATEBACK_LOG,
                    LogKey(operation).toByteIterable(),
                    serialize(operation).toByteIterable()
            )
            tx.commit()
        }
    }

    override fun getAllPerformedDatebackOperations(): List<DatebackOperation> {
        val resultList = mutableListOf<DatebackOperation>()
        this.owningDb.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__DATEBACK_LOG).use { cursor ->
                while(cursor.next){
                    val op = this.deserialize(cursor.value.toByteArray()) as DatebackOperation
                    resultList += op
                }
            }
        }
        return resultList
    }

    override fun getDatebackOperationsPerformedOnBranchBetween(branch: String, dateTimeMin: Long, dateTimeMax: Long): List<DatebackOperation> {
        val resultList = mutableListOf<DatebackOperation>()
        this.owningDb.globalChunkManager.openReadOnlyTransactionOnGlobalEnvironment().use { tx ->
            tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__DATEBACK_LOG).use { cursor ->
                if(cursor.ceilKey(LogKey(dateTimeMin, branch, "").toByteIterable()) != null){
                    while(cursor.key.parseAsLogKey().let { it.branch == branch && it.wallClockTime >= dateTimeMin && it.wallClockTime <= dateTimeMax }){
                        resultList += this.deserialize(cursor.value.toByteArray()) as DatebackOperation
                        if(!cursor.next){
                            break
                        }
                    }
                }
            }
        }
        return resultList
    }

    override fun compactStorage(branch: String) {
        super.compactStorage(branch)
        this.owningDb.globalChunkManager.compactAllExodusEnvironmentsForBranch(branch)
    }

    override fun deleteLogsForBranch(branchName: String) {
        this.owningDb.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val keysToRemove = mutableListOf<ByteIterable>()
            tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__DATEBACK_LOG).use { cursor ->
                while(cursor.next){
                    val key = cursor.key.parseAsLogKey()
                    if(key.branch == branchName){
                        keysToRemove += cursor.key
                    }
                }
            }
            keysToRemove.forEach{ key -> tx.delete(ChronoDBStoreLayout.STORE_NAME__DATEBACK_LOG, key)}
            tx.commit()
        }
    }

    // =================================================================================================================
    // HELPER FUNCTIONS
    // =================================================================================================================

    private val owningDb: ExodusChronoDB
        get() = super.getOwningDb() as ExodusChronoDB

    private fun serialize(obj: Any): ByteArray {
        return this.owningDb.serializationManager.serialize(obj)
    }

    private fun deserialize(obj: ByteArray): Any {
        return this.owningDb.serializationManager.deserialize(obj)
    }


    private fun ByteIterable.parseAsLogKey(): LogKey {
        // the byte-array form looks like this:
        //
        // [branch name bytes][8 bytes for timestamp (long)][16 bytes (ID)]
        //
        // This preserves the comparison ordering.
        val timePart = this.subIterable(this.length - 8 - 16, 8)
        val timestamp = LongBinding.entryToLong(timePart)
        val branchPart = this.subIterable(0, this.length - 8 - 16)
        val branch = StringBinding.entryToString(branchPart)
        val idPart = this.subIterable(this.length - 16, 16)
        val id = if(idPart.consistsOfZeroes()) {
            ""
        } else {
            idPart.parseAsUUID().toString()
        }
        return LogKey(timestamp, branch, id)
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class LogKey(val wallClockTime: Long, val branch: String, val id: String) {

        constructor(operation: DatebackOperation) : this(operation.wallClockTime, operation.branch, operation.id)

        fun toByteIterable(): ByteIterable {
            // the byte-array form looks like this:
            //
            // [branch name bytes][8 bytes for timestamp (long)][16 bytes (ID)]
            //
            // This preserves the comparison ordering.
            val branch = StringBinding.stringToEntry(this.branch)
            val time = LongBinding.longToEntry(this.wallClockTime)
            val id = if(this.id == "") {
                 ByteArray(16, { pos -> 0 }).toByteIterable()
            } else {
                UUID.fromString(this.id).toByteIterable()
            }
            val jointArray = ByteArray(branch.length + time.length + id.length)
            System.arraycopy(branch.bytesUnsafe, 0, jointArray, 0, branch.length)
            System.arraycopy(time.bytesUnsafe, 0, jointArray, branch.length, time.length)
            System.arraycopy(id.bytesUnsafe, 0, jointArray, branch.length + time.length, id.length)
            return ArrayByteIterable(jointArray)
        }
    }

}