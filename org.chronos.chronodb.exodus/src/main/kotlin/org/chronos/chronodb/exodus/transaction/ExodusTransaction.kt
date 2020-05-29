package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable

interface ExodusTransaction {

    fun openCursorOn(store: String): Cursor

    fun rollback()

    fun commit()

    val isOpen: Boolean

    fun put(store: String, key: ByteIterable, value: ByteIterable): Boolean

    fun put(store: String, key: String, value: ByteIterable): Boolean {
        return this.put(store, key.toByteIterable(), value)
    }

    fun get(store: String, key: ByteIterable): ByteIterable?

    fun get(store: String, key: String): ByteIterable? {
        return this.get(store, key.toByteIterable())
    }

    fun delete(store: String, key: ByteIterable): Boolean

    fun delete(store: String, key: String): Boolean {
        return this.delete(store, key.toByteIterable())
    }

    fun <T> use(consumer: (ExodusTransaction) -> T): T {
        try {
            return consumer(this)
        } finally {
            if (this.isOpen) {
                this.rollback()
            }
        }
    }

    fun <T> withCursorOn(store: String, consumer: (Cursor) -> T): T {
        if(!this.storeExists(store)){
            return consumer(EmptyCursor)
        }
        return this.openCursorOn(store).use(consumer)
    }

    fun storeSize(store: String): Long

    fun getAllStoreNames(): List<String>

    fun storeExists(storeName: String): Boolean

    fun truncateStore(storeName: String)

    fun removeStore(storeName: String)

    fun flush(): Boolean

    val environmentLocation: String
}