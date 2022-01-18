package org.chronos.chronodb.test.cases.engine.query

import org.chronos.chronodb.api.Order
import org.chronos.chronodb.inmemory.IndexEntry
import org.chronos.chronodb.inmemory.IndexKey
import org.chronos.chronodb.inmemory.RawInMemoryIndexCursor
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.index.cursor.DeltaResolvingScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IndexScanCursor
import org.chronos.chronodb.internal.impl.index.cursor.IteratorWrappingIndexScanCursor
import org.junit.Assert.*
import org.junit.Test

class DeltaResolvingScanCursorTest {

    @Test
    fun canResolveDeltasAscending() {
        val parentBranchContent = listOf(
            "Apple" to "p1",
            "Apple" to "p3",
            "Banana" to "p1",
            "Strawberry" to "p2",
        )
        val childBranchContent = listOf(
            // add "acerola" to "p2"
            IndexEntry(IndexKey("Acerola", "p2"), listOf(Period.eternal())),
            // delete "apple" from "p1".
            IndexEntry(IndexKey("Apple", "p1"), listOf(Period.createRange(0, 900))),
            // add "pineapple" to "p1".
            IndexEntry(IndexKey("Pineapple", "p1"), listOf(Period.eternal())),
            // add "strawberry" to "p4"
            IndexEntry(IndexKey("Strawberry", "p4"), listOf(Period.eternal()))
        )

        val parentCursor = IteratorWrappingIndexScanCursor(parentBranchContent.iterator(), Order.ASCENDING)
        val childCursor = RawInMemoryIndexCursor<String>(childBranchContent.iterator(), Order.ASCENDING)

        val deltaCursor = DeltaResolvingScanCursor(parentCursor, 1000, childCursor)

        val actual = deltaCursor.toList()
        val expected = listOf(
            "Acerola" to "p2",
            "Apple" to "p3",
            "Banana" to "p1",
            "Pineapple" to "p1",
            "Strawberry" to "p2",
            "Strawberry" to "p4"
        )

        assertEquals(expected, actual)
    }

    @Test
    fun canResolveDeltasDescending() {
        val parentBranchContent = listOf(
            "Strawberry" to "p2",
            "Banana" to "p1",
            "Apple" to "p3",
            "Apple" to "p1",
        )
        val childBranchContent = listOf(
            // add "strawberry" to "p4"
            IndexEntry(IndexKey("Strawberry", "p4"), listOf(Period.eternal())),
            // add "pineapple" to "p1".
            IndexEntry(IndexKey("Pineapple", "p1"), listOf(Period.eternal())),
            // delete "apple" from "p1".
            IndexEntry(IndexKey("Apple", "p1"), listOf(Period.createRange(0, 900))),
            // add "acerola" to "p2"
            IndexEntry(IndexKey("Acerola", "p2"), listOf(Period.eternal())),
        )

        val parentCursor = IteratorWrappingIndexScanCursor(parentBranchContent.iterator(), Order.DESCENDING)
        val childCursor = RawInMemoryIndexCursor<String>(childBranchContent.iterator(), Order.DESCENDING)

        val deltaCursor = DeltaResolvingScanCursor(parentCursor, 1000, childCursor)

        val actual = deltaCursor.toList()
        val expected = listOf(
            "Strawberry" to "p4",
            "Strawberry" to "p2",
            "Pineapple" to "p1",
            "Banana" to "p1",
            "Apple" to "p3",
            "Acerola" to "p2",
        )

        assertEquals(expected, actual)
    }

    private fun <V : Comparable<V>> IndexScanCursor<V>.toList(): List<Pair<V, String>> {
        val list = mutableListOf<Pair<V, String>>()
        while (this.next()) {
            list += Pair(this.indexValue, this.primaryKey)
        }
        return list
    }

}