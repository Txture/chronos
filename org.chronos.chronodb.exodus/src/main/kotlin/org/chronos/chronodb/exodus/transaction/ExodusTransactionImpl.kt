package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction

class ExodusTransactionImpl : ExodusTransaction {

    private val environment: Environment
    private val transaction: Transaction

    constructor(environment: Environment, transaction: Transaction) {
        this.environment = environment
        this.transaction = transaction
    }

    override fun openCursorOn(store: String): Cursor {
        if (this.transaction.isReadonly && !this.storeExists(store)) {
            return EmptyCursor
        }
        val storeObject = this.environment.openStore(store, StoreConfig.WITHOUT_DUPLICATES, this.transaction)
        return storeObject.openCursor(this.transaction)
    }

    override fun rollback() {
        this.transaction.abort()
    }

    override fun commit() {
        this.transaction.commit()
    }

    override val isOpen
        get() = !this.transaction.isFinished

    override fun storeSize(store: String): Long {
        if (!this.storeExists(store)) {
            return 0L
        }
        val storeObject = this.environment.openStore(store, StoreConfig.WITHOUT_DUPLICATES, this.transaction)
        return storeObject.count(this.transaction)
    }

    override fun put(store: String, key: ByteIterable, value: ByteIterable): Boolean {
        val storeObject = this.environment.openStore(store, StoreConfig.WITHOUT_DUPLICATES, this.transaction)
        return storeObject.put(this.transaction, key, value)
    }

    override fun get(store: String, key: ByteIterable): ByteIterable? {
        if (this.transaction.isReadonly && !this.storeExists(store)) {
            return null
        }
        val storeObject = this.environment.openStore(store, StoreConfig.WITHOUT_DUPLICATES, this.transaction)
        return storeObject.get(this.transaction, key)
    }

    override fun delete(store: String, key: ByteIterable): Boolean {
        val storeObject = this.environment.openStore(store, StoreConfig.WITHOUT_DUPLICATES, this.transaction)
        return storeObject.delete(this.transaction, key)
    }

    override fun getAllStoreNames(): List<String> {
        return this.environment.getAllStoreNames(this.transaction)
    }

    override fun storeExists(storeName: String): Boolean {
        return this.environment.storeExists(storeName, this.transaction)
    }

    override fun truncateStore(storeName: String) {
        this.environment.truncateStore(storeName, this.transaction)
    }

    override fun removeStore(storeName: String) {
        this.environment.removeStore(storeName, this.transaction)
    }

    override fun flush(): Boolean {
        return this.transaction.flush()
    }

    override val environmentLocation: String
        get() = this.transaction.environment.location

}