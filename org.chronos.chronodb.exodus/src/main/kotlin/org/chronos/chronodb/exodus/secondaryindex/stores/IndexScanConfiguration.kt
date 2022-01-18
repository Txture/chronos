package org.chronos.chronodb.exodus.secondaryindex.stores

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor
import mu.KotlinLogging
import org.chronos.chronodb.exodus.kotlin.ext.ceilEntry
import org.chronos.chronodb.exodus.kotlin.ext.floorEntry
import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.ASCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanDirection.DESCENDING
import org.chronos.chronodb.exodus.secondaryindex.stores.IndexScanConfiguration.ScanStrategy
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification

/**
 * @param tx The transaction to operate on.
 * @param searchSpec The search specification to evaluate.
 * @param timestamp The timestamp to perform the search at. Must be non-negative.
 * @param storeName The name of the Exodus store to scan.
 * @param scanStart The position to start the scan at. Next lower/higher position will be chosen accordingly if not present, depending on scan direction. If `null`, a full-table scan will be performed.
 * @param direction The scan direction
 * @param scanStrategy Indicates when to stop scanning.
 * @param skip A check function that allows to skip over entries without cancelling the scan (in case of [ScanStrategy.STOP_AT_FIRST_MISMATCH]).
 * @param parseKey A function that parses the secondary index keys in the store.
 */
data class IndexScanConfiguration<V, S : SearchSpecification<V, *>>(
    val tx: ExodusTransaction,
    val searchSpec: S,
    val timestamp: Long,
    val storeName: String,
    val scanStart: ByteIterable?,
    val direction: ScanDirection,
    val scanStrategy: ScanStrategy,
    val skip: (V) -> Boolean = { false },
    val parseKey: (ByteIterable) -> SecondaryIndexKey<V>,
    val scanTimeMode: ScanTimeMode
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }


    init {
        requireNonNegative(this.timestamp, "timestamp")
    }

    fun moveCursorToInitialPosition(cursor: Cursor): Boolean {
        return when (this.direction) {
            ASCENDING -> moveCursorToInitialPositionAscending(cursor)
            DESCENDING -> moveCursorToInitialPositionDescending(cursor)
        }
    }

    private fun moveCursorToInitialPositionAscending(cursor: Cursor): Boolean {
        return when (this.scanStart) {
            null -> cursor.next // start at the lowest key/value pair
            else -> cursor.ceilEntry(this.scanStart) != null  // start at the given position
        }
    }

    private fun moveCursorToInitialPositionDescending(cursor: Cursor): Boolean {
        return when (this.scanStart) {
            null -> cursor.last // start at the highest key/value pair
            else -> cursor.floorEntry(this.scanStart) != null
        }
    }

    fun performScan(): List<ScanResultEntry<V>> {
        // unpack the scan configuration
        if (!tx.storeExists(storeName)) {
            // the store doesn't exist, so this secondary index is empty.
            // Note that this is a valid case: the store may not have been
            // created if there were no entries for it; it is NOT an error!
            return listOf()
        }
        return tx.withCursorOn(storeName) { cursor ->
            val resultList = mutableListOf<ScanResultEntry<V>>()
            if (!this.moveCursorToInitialPosition(cursor)) {
                // failed to initialize cursor -> there is no matching entry in the index
                return@withCursorOn resultList
            }
            var previousBinaryIndexValue: ByteIterable? = null
            var keepGoing: Boolean
            var scannedRows = 0
            val timeBefore = System.currentTimeMillis()
            do {
                scannedRows++
                // parse the key
                val secondaryIndexKey = parseKey(cursor.key)
                val sameIndexValue = secondaryIndexKey.indexValueBinary == previousBinaryIndexValue
                if (!sameIndexValue) {
                    // it's a different index value than before, parse it and check it
                    val isMatch = searchSpec.matches(secondaryIndexKey.indexValuePlain)
                    if (!isMatch) {
                        // key didn't match
                        previousBinaryIndexValue = null
                        val shouldSkip = skip(secondaryIndexKey.indexValuePlain)
                        if (!shouldSkip) {
                            // we needed this value to match, but it didn't...
                            if (scanStrategy == ScanStrategy.SCAN_UNTIL_END) {
                                keepGoing = direction.moveCursor(cursor)
                            } else {
                                keepGoing = false
                            }
                        } else {
                            // we skip over this value and continue on
                            keepGoing = direction.moveCursor(cursor)
                        }
                        continue
                    }
                }
                // at this point, we know that the key itself matches,
                // so we check the timestamps
                if (StoreUtils.isTimestampInRange(timestamp, cursor.value, scanTimeMode)) {
                    resultList.add(secondaryIndexKey.toScanResultEntry())
                }
                // remember that this binary key matched
                previousBinaryIndexValue = secondaryIndexKey.indexValueBinary
                // move the cursor (if there is another entry)
                keepGoing = direction.moveCursor(cursor)
            } while (keepGoing)

            val timeAfter = System.currentTimeMillis()
            log.trace {  "Scanned ${scannedRows} rows in ${timeAfter - timeBefore}ms, produced ${resultList.size} results. Query: ${searchSpec}" }

            return@withCursorOn resultList
        }
    }

    enum class ScanDirection {

        ASCENDING(Cursor::getNext), DESCENDING(Cursor::getPrev);

        private val moveFunction: (Cursor) -> Boolean

        constructor(moveFunction: (Cursor) -> Boolean) {
            this.moveFunction = moveFunction
        }

        fun moveCursor(cursor: Cursor): Boolean {
            return this.moveFunction(cursor)
        }
    }

    enum class ScanStrategy {

        STOP_AT_FIRST_MISMATCH,
        SCAN_UNTIL_END

    }
}

