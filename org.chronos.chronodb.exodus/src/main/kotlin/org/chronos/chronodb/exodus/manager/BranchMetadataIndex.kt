package org.chronos.chronodb.exodus.manager

import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.transaction.ExodusTransaction

object BranchMetadataIndex {

    fun values(tx: ExodusTransaction): List<ByteArray> {
        val result = mutableListOf<ByteArray>()
        tx.openCursorOn(ChronoDBStoreLayout.STORE_NAME__BRANCH_METADATA).use { cursor ->
            while(cursor.next){
                result.add(cursor.value.toByteArray())
            }
        }
        return result
    }


    fun insertOrUpdate(tx: ExodusTransaction, name: String, metadata: ByteArray) {
        tx.put(ChronoDBStoreLayout.STORE_NAME__BRANCH_METADATA, name, metadata.toByteIterable())
    }

    fun getMetadata(tx: ExodusTransaction, name: String): ByteArray? {
        return tx.get(ChronoDBStoreLayout.STORE_NAME__BRANCH_METADATA, name)?.toByteArray()
    }

    fun deleteBranch(tx: ExodusTransaction, name: String) {
        tx.delete(ChronoDBStoreLayout.STORE_NAME__BRANCH_METADATA, name)
    }

}