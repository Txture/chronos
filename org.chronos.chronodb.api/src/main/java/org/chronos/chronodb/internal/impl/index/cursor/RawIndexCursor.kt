package org.chronos.chronodb.internal.impl.index.cursor

import org.chronos.chronodb.api.Order

/**
 * A [RawIndexCursor] is the basic building block for iterating over a secondary index.
 *
 * In contrast to the more sophisticated [IndexScanCursor], the [RawIndexCursor] returns
 * ALL entries of the backing store unfiltered.
 *
 * This interface also offers an [isVisibleForTimestamp] check that allows the user to
 * check the current entry for visibility.
 */
interface RawIndexCursor<V: Comparable<V>>: AutoCloseable {

    fun next(): Boolean

    val primaryKey: String

    val indexValue: V

    val order: Order

    fun isVisibleForTimestamp(timestamp: Long): Boolean

}