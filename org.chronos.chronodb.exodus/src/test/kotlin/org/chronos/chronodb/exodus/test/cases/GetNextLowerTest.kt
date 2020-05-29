package org.chronos.chronodb.exodus.test.cases

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.kotlin.ext.floorAndHigherEntry
import org.chronos.chronodb.exodus.kotlin.ext.floorKey
import org.chronos.chronodb.exodus.kotlin.ext.mapSingle
import org.chronos.chronodb.exodus.kotlin.ext.readFrom
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.Test


class GetNextLowerTest : EnvironmentTest() {

    @Test
    fun testFloorKey() {
        environment.executeInTransaction { tx ->
            val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
            store.put(tx, 1, 1)
            store.put(tx, 2, 2)
            store.put(tx, 7, 7)
            store.put(tx, 9, 9)
        }

        environment.executeInReadonlyTransaction { tx ->
            val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
            store.openCursor(tx).use { cursor ->
                val leq0 = cursor.floorKey(0.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq1 = cursor.floorKey(1.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq2 = cursor.floorKey(2.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq3 = cursor.floorKey(3.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq4 = cursor.floorKey(4.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq5 = cursor.floorKey(5.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq6 = cursor.floorKey(6.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq7 = cursor.floorKey(7.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq8 = cursor.floorKey(8.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq9 = cursor.floorKey(9.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq10 = cursor.floorKey(10.binary()).mapSingle(IntegerBinding::entryToInt)
                val leq11 = cursor.floorKey(11.binary()).mapSingle(IntegerBinding::entryToInt)

                leq0 shouldBe nullValue()
                leq1 shouldBe 1
                leq2 shouldBe 2
                leq3 shouldBe 2
                leq4 shouldBe 2
                leq5 shouldBe 2
                leq6 shouldBe 2
                leq7 shouldBe 7
                leq8 shouldBe 7
                leq9 shouldBe 9
                leq10 shouldBe 9
                leq11 shouldBe 9
            }
        }
    }

    @Test
    fun testFloorAndHigherEntry() {
        environment.executeInTransaction { tx ->
            val store = environment.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
            store.put(tx, 1, 1)
            store.put(tx, 2, 2)
            store.put(tx, 7, 7)
            store.put(tx, 9, 9)
        }

        environment.readFrom("test", StoreConfig.WITHOUT_DUPLICATES) { cursor ->
            val (floor0, ceil0) = cursor.floorAndHigherEntry(0.binary()).toIntKeyPair()
            val (floor1, ceil1) = cursor.floorAndHigherEntry(1.binary()).toIntKeyPair()
            val (floor2, ceil2) = cursor.floorAndHigherEntry(2.binary()).toIntKeyPair()
            val (floor3, ceil3) = cursor.floorAndHigherEntry(3.binary()).toIntKeyPair()
            val (floor4, ceil4) = cursor.floorAndHigherEntry(4.binary()).toIntKeyPair()
            val (floor5, ceil5) = cursor.floorAndHigherEntry(5.binary()).toIntKeyPair()
            val (floor6, ceil6) = cursor.floorAndHigherEntry(6.binary()).toIntKeyPair()
            val (floor7, ceil7) = cursor.floorAndHigherEntry(7.binary()).toIntKeyPair()
            val (floor8, ceil8) = cursor.floorAndHigherEntry(8.binary()).toIntKeyPair()
            val (floor9, ceil9) = cursor.floorAndHigherEntry(9.binary()).toIntKeyPair()
            val (floor10, ceil10) = cursor.floorAndHigherEntry(10.binary()).toIntKeyPair()
            val (floor11, ceil11) = cursor.floorAndHigherEntry(11.binary()).toIntKeyPair()

            floor0 shouldBe nullValue(); ceil0 shouldBe 1
            floor1 shouldBe 1; ceil1 shouldBe 2
            floor2 shouldBe 2; ceil2 shouldBe 7
            floor3 shouldBe 2; ceil3 shouldBe 7
            floor4 shouldBe 2; ceil4 shouldBe 7
            floor5 shouldBe 2; ceil5 shouldBe 7
            floor6 shouldBe 2; ceil6 shouldBe 7
            floor7 shouldBe 7; ceil7 shouldBe 9
            floor8 shouldBe 7; ceil8 shouldBe 9
            floor9 shouldBe 9; ceil9 shouldBe nullValue()
            floor10 shouldBe 9; ceil10 shouldBe nullValue()
            floor11 shouldBe 9; ceil11 shouldBe nullValue()

        }
    }

    private fun Store.put(tx: Transaction, key: Int, value: Int) {
        this.put(tx, key.binary(), value.binary())
    }

    private fun Int.binary(): ByteIterable {
        return IntegerBinding.intToEntry(this)
    }

    private fun ByteIterable.toInt(): Int {
        return IntegerBinding.entryToInt(this)
    }

    private fun Pair<Pair<ByteIterable, ByteIterable>?, Pair<ByteIterable, ByteIterable>?>.toIntKeyPair(): Pair<Int?, Int?> {
        return Pair(this.first.mapSingle { it.first.toInt() }, this.second.mapSingle { it.second.toInt() })
    }
}