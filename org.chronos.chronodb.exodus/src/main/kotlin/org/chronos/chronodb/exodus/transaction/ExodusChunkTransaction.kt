package org.chronos.chronodb.exodus.transaction

import org.chronos.chronodb.internal.api.Period

interface ExodusChunkTransaction : ExodusTransaction {

    val chunkValidPeriod: Period

    fun <T> useChunkTx(consumer: (ExodusChunkTransaction) -> T): T {
        try {
            return consumer(this)
        } finally {
            if (this.isOpen) {
                this.rollback()
            }
        }
    }
}