package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order
import org.slf4j.LoggerFactory

/**
 * An [IndexScanCursor] is a cursor over a secondary index which only returns
 * entries that are visible at the timestamp it is observing.
 *
 * The general usage is:
 *
 * ```
 * try(Cursor cursor = ...){
 *     while(cursor.next()){
 *         String primaryKey = cursor.getPrimaryKey();
 *         Comparable<?> indexValue = cursor.getIndexValue();
 *         // use the primary key and index value
 *     }
 * }
 * ```
 *
 * Please note that a cursor **must be [close]d**. Failing to call close()
 * may lead to resource leaks.
 *
 * @author martin.haeusler@txture.io -- Initial Contribution and API
 */
abstract class IndexScanCursor<V : Comparable<V>> : AutoCloseable {

    private companion object {

        @JvmStatic
        private val log = LoggerFactory.getLogger(IndexScanCursor::class.java)

    }

    private var onCloseActions = mutableListOf<(() -> Unit)>()
    private var initialized = false
    private var closed = false
    private var exhausted = false

    abstract val primaryKey: String

    abstract val indexValue: V

    abstract val order: Order

    protected abstract fun nextInternal(): Boolean

    fun next(): Boolean {
        if (!this.initialized) {
            this.initialized = true
        }
        this.exhausted = !this.nextInternal()
        return !this.exhausted
    }


    fun onClose(action: () -> Unit) {
        this.onCloseActions.add(action)
    }

    final override fun close() {
        if (closed) {
            return
        }
        closed = true
        this.closeInternal()
        this.onCloseActions.forEach { it() }
    }

    protected abstract fun closeInternal()

    protected fun assertNotClosed() {
        if (closed) {
            throw IllegalStateException("This cursor has already been closed!")
        }
    }

    fun filter(predicate: (Pair<V, String>) -> Boolean): IndexScanCursor<V> {
        return FilteringScanCursor(this, predicate)
    }

    protected fun finalize() {
        if (!this.closed) {
            log.warn(
                "Closing orphan IndexScanCursor in finalizer, which" +
                    " signifies a potential resource leak. Please call" +
                    " close() on cursors, or use try-with-resources" +
                    " (Java) or .use{ } (Kotlin)."
            )
            this.close()
        }
    }
}