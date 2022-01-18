package org.chronos.chronodb.exodus.migration

import com.google.common.collect.Maps
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.layout.ChronoDBStoreLayout
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.migration.ChronosMigration
import org.chronos.chronodb.internal.api.migration.annotations.Migration

@Migration(from = "0.9.62", to = "0.9.71")
class ExodusMigration0_9_62_to_0_9_71 : ChronosMigration<ExodusChronoDB> {

    override fun execute(chronoDB: ExodusChronoDB) {
        // Exodus ChronoDB had an issue with secondary indices being incorrect,
        // which was fixed in 0.9.70. In order for this fix to take effect, we
        // need to perform a reindexing on all branches.

        // As we have no direct access to the index manager here for that purpose,
        // we'll mark all indices as dirty instead. This will prevent their usage.
        chronoDB.globalChunkManager.openReadWriteTransactionOnGlobalEnvironment().use { tx ->
            val oldIndexDirtyStates = this.loadIndexDirtyStates(chronoDB, tx)
            val newIndexDirtyStates = oldIndexDirtyStates.keys.asSequence().map { it to true }.toMap()
            this.persistIndexDirtyStates(chronoDB, tx, newIndexDirtyStates)
            tx.commit()
        }
    }

    private fun loadIndexDirtyStates(chronoDB: ExodusChronoDB, tx: ExodusTransaction): Map<String, Boolean> {
        return run {
            val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY
            val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
            tx.get(indexName, key)?.toByteArray()
                ?.let { chronoDB.serializationManager.deserialize(it) as Map<String, Boolean> }
        }
            ?: emptyMap()
    }

    private fun persistIndexDirtyStates(chronoDB: ExodusChronoDB, tx: ExodusTransaction, indexNameToDirtyFlag: Map<String, Boolean>) {
        val serializedForm = chronoDB.serializationManager.serialize(Maps.newHashMap(indexNameToDirtyFlag))
        val indexName = ChronoDBStoreLayout.STORE_NAME__INDEXDIRTY
        val key = ChronoDBStoreLayout.KEY__ALL_INDEXERS
        tx.put(indexName, key, serializedForm.toByteIterable())
    }


}