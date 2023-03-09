package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.log.TooBigLoggableException
import jetbrains.exodus.util.HexUtil
import org.chronos.chronodb.exodus.kotlin.ext.parseAsUnqualifiedTemporalKey
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray

class ExodusTransactionImpl : ExodusTransaction {

    companion object {

        // maximum entry size in Exodus is 8MB, but not everything is usable due to overhead.
        private const val MAX_EXODUS_ENTRY_SIZE_IN_BYTES = 8 * 1024 * 1024 - 7 /* overhead */

    }


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
        if ((key.length + value.length) > MAX_EXODUS_ENTRY_SIZE_IN_BYTES) {
            val keyString = try {
                key.parseAsUnqualifiedTemporalKey()
            } catch (e: Exception) {
                try {
                    String(key.toByteArray())
                } catch (e: Exception) {
                    HexUtil.byteArrayToString(key.toByteArray())
                }
            }
            throw TooBigLoggableException("The given key-value pair is too large to be stored (maximum size is 8MB = ${MAX_EXODUS_ENTRY_SIZE_IN_BYTES} bytes, given entry has ${key.length + value.length} bytes). Key: ${keyString}, Store: ${store}")
        }
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