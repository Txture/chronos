package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.internal.api.Period

class ExodusChunkTransactionImpl : ExodusChunkTransaction {


    val exodusTransaction: ExodusTransaction
    override val chunkValidPeriod: Period

    constructor(exodusTransaction: ExodusTransaction, chunkValidPeriod: Period){
        this.exodusTransaction = exodusTransaction
        this.chunkValidPeriod = chunkValidPeriod
    }

    override fun openCursorOn(store: String): Cursor {
        return this.exodusTransaction.openCursorOn(store)
    }

    override fun rollback() {
        this.exodusTransaction.rollback()
    }

    override fun commit() {
        this.exodusTransaction.commit()
    }

    override val isOpen: Boolean
        get() = this.exodusTransaction.isOpen

    override fun storeSize(store: String): Long {
        return this.exodusTransaction.storeSize(store)
    }

    override fun put(store: String, key: ByteIterable, value: ByteIterable): Boolean {
        return this.exodusTransaction.put(store, key, value)
    }

    override fun get(store: String, key: ByteIterable): ByteIterable? {
        return this.exodusTransaction.get(store, key)
    }

    override fun delete(store: String, key: ByteIterable): Boolean {
        return this.exodusTransaction.delete(store, key)
    }

    override fun getAllStoreNames(): List<String> {
        return this.exodusTransaction.getAllStoreNames()
    }

    override fun storeExists(storeName: String): Boolean {
        return this.exodusTransaction.storeExists(storeName)
    }

    override fun truncateStore(storeName: String) {
        return this.exodusTransaction.truncateStore(storeName)
    }

    override fun removeStore(storeName: String) {
        this.exodusTransaction.removeStore(storeName)
    }

    override fun flush(): Boolean {
        return this.exodusTransaction.flush()
    }

    override val environmentLocation: String
        get() = this.exodusTransaction.environmentLocation
}