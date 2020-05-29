package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Transaction

interface ExodusTransactionSource {

    fun openReadOnlyTransaction(branch: String, timestamp: Long): ExodusTransaction

    fun openReadWriteTransaction(branch: String, timestamp: Long): ExodusTransaction


    fun <T> readFrom(branch: String, timestamp: Long, storeName: String, consumer: (Cursor) -> T): T {
        val tx = this.openReadOnlyTransaction(branch, timestamp)
        try {
            return tx.openCursorOn(storeName).use(consumer)
        } finally {
            tx.rollback()
        }
    }

    fun <T> executeInReadonlyTransaction(branch: String, timestamp: Long, consumer: (ExodusTransaction) -> T): T {
        val tx = this.openReadOnlyTransaction(branch, timestamp)
        try {
            return consumer(tx)
        } finally {
            tx.rollback()
        }
    }

    fun <T> executeInExclusiveTransaction(branch: String, timestamp: Long, consumer: (ExodusTransaction) -> T): T {
        val tx = this.openReadWriteTransaction(branch, timestamp)
        try {
            return consumer(tx)
        } finally {
            tx.rollback()
        }
    }



}