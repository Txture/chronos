package org.chronos.chronodb.exodus.secondaryindex

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanTimeMode
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryIndexKey
import org.chronos.chronodb.exodus.secondaryindex.stores.StoreUtils
import org.chronos.chronodb.internal.impl.index.cursor.RawIndexCursor

class ExodusRawIndexCursor<V: Comparable<V>>(
    private val cursor: Cursor,
    override val order: Order,
    private val parseKey: (ByteIterable) -> SecondaryIndexKey<V>
): RawIndexCursor<V> {

    private var currentKey: SecondaryIndexKey<V>? = null

    init {
        if(this.order == Order.DESCENDING){
            // by default, Exodus cursors are ascending and start
            // at the first entry. If we want descending sorting,
            // we need to start at the last entry instead and go backwards.
            this.currentKey = if(this.cursor.last){
                this.parseKey(this.cursor.key)
            }else{
                null
            }
        }
    }

    override fun next(): Boolean {
        val result = when(this.order){
            Order.ASCENDING -> this.cursor.next
            Order.DESCENDING -> this.cursor.prev
        }
        currentKey = null
        return result
    }

    override val primaryKey: String
        get() = this.getParsedKey().primaryKeyPlain

    override val indexValue: V
        get() = this.getParsedKey().indexValuePlain

    override fun isVisibleForTimestamp(timestamp: Long): Boolean {
        return StoreUtils.isTimestampInRange(timestamp, this.cursor.value, ScanTimeMode.SCAN_FOR_PERIOD_MATCHES)
    }

    override fun close() {
        this.cursor.close()
    }

    private fun getParsedKey(): SecondaryIndexKey<V>{
        val key = currentKey ?: this.parseKey(this.cursor.key)
        this.currentKey = key
        return key
    }

}