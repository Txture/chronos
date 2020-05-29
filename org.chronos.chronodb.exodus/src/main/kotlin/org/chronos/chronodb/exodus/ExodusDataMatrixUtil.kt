package org.chronos.chronodb.exodus

import jetbrains.exodus.bindings.BooleanBinding
import org.chronos.chronodb.exodus.kotlin.ext.isEmpty
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry

object ExodusDataMatrixUtil {

    fun insertEntries(tx: ExodusTransaction, storeName: String, entries: Set<UnqualifiedTemporalEntry>) {
        val inverseStore = storeName + TemporalExodusMatrix.INVERSE_STORE_NAME_SUFFIX
        entries.forEach { entry ->
            val tKey = entry.key.toByteIterable()
            val value = entry.value.toByteIterable()
            val inverseKey = entry.key.inverse().toByteIterable()
            val inverseValue = BooleanBinding.booleanToEntry(!value.isEmpty())
            tx.put(storeName, tKey, value)
            tx.put(inverseStore, inverseKey, inverseValue)
        }
    }

}