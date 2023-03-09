package org.chronos.chronodb.exodus.migration

import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.secondaryindex.ExodusSecondaryIndex
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.migration.ChronosMigration
import org.chronos.chronodb.internal.api.migration.annotations.Migration
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.common.serialization.KryoManager

@Migration(from = "1.2.27", to = "1.3.0")
class ExodusMigration1_2_27_to_1_3_0 : ChronosMigration<ExodusChronoDB> {

    override fun execute(chronoDB: ExodusChronoDB) {
        // In Xodus 1.x, there was a bug in "DoubleBinding" which caused byte arrays produced
        // by this class to result in a wrong sort order if the input doubles were negative values.
        // This issue was fixed in Xodus 2.x with the introduction of "SignedDoubleBinding" which
        // produces different byte arrays which work for positive and negative input values.
        //
        // In order to make use of this, we have to mark all existing secondary indices of type Double as dirty.
        chronoDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val doubleIndices = getIndexersSerialForm(tx).asSequence()
                .map { it.parseSecondaryIndex() }
                .filter { it.valueType == Double::class.java }
                .filterNot { it.dirty } // we don't care about indices which are already dirty here.
                .toSet()

            if (doubleIndices.isEmpty()) {
                return@use
            }

            for (doubleIndex in doubleIndices) {
                (doubleIndex as SecondaryIndexImpl).dirty = true
            }


            this.persistIndexSet(tx, doubleIndices)
            tx.commit()

            chronoDB.globalChunkManager.dropSecondaryIndexFiles(doubleIndices)
        }

        // note: this migration leaves the formerly clean indices in a dirty state and requires reindexing.
    }

    private fun getIndexersSerialForm(tx: ExodusTransaction): List<ByteArray> {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        val resultList = mutableListOf<ByteArray>()
        tx.withCursorOn(indexName) { cursor ->
            while (cursor.next) {
                resultList += cursor.value.toByteArray()
            }
        }
        return resultList
    }

    private fun ByteArray.parseSecondaryIndex(): SecondaryIndex {
        val serializableForm = KryoManager.deserialize<ExodusSecondaryIndex>(this)
        return serializableForm.toSecondaryIndex()
    }

    private fun persistIndexSet(tx: ExodusTransaction, indices: Set<SecondaryIndex>) {
        this.saveIndexers(tx, indices.asSequence().map { it.id to it.toByteArray() }.toMap())
    }

    private fun saveIndexers(tx: ExodusTransaction, idToSerialForm: Map<String, ByteArray>) {
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXERS
        for ((id, serialForm) in idToSerialForm) {
            tx.put(indexName, id, serialForm.toByteIterable())
        }
    }

    private fun SecondaryIndex.toByteArray(): ByteArray {
        val serializableForm = ExodusSecondaryIndex(this)
        return KryoManager.serialize(serializableForm)
    }
}