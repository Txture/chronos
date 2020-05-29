package org.chronos.chronodb.exodus.test.cases.secondaryindex

import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.NumberCondition
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.secondaryindex.stores.*
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.transaction.ExodusTransactionImpl
import org.chronos.chronodb.exodus.util.readLongsFromBytes
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification
import org.chronos.chronodb.internal.impl.query.LongSearchSpecificationImpl
import org.junit.jupiter.api.Test

class SecondaryLongIndexStoreTest : EnvironmentTest() {

    @Test
    fun canInsertIntoStore() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1112", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1113", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 575, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 625, "3333", 2000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 736, "4444", 2000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexKeys = this.readOnlyTx { tx -> readKeys(tx, SecondaryLongIndexStore.storeName("value", "default")) }

        // check that the contents are correct
        indexKeys shouldBe listOf(
            ScanResultEntry(314L, "1111"),
            ScanResultEntry(314L, "1112"),
            ScanResultEntry(314L, "1113"),
            ScanResultEntry(575L, "2222"),
            ScanResultEntry(625L, "3333"),
            ScanResultEntry(736L, "4444")
        )
    }

    @Test
    fun canTerminateValidityPeriods() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1112", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 425, "2222", 1000)
            SecondaryLongIndexStore.terminateValidity(tx, "value", "default", 314, "1112", 2000, 0L)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1112", 3000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryLongIndexStore.storeName("value", "default")) }
        indexEntries shouldBe listOf(
            Triple(314L, "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple(314L, "1112", listOf(1000L, 2000L, 3000L, Long.MAX_VALUE)),
            Triple(425L, "2222", listOf(1000L, Long.MAX_VALUE))
        )
    }

    @Test
    fun terminationDuringIncrementalCommitsWorks() {
        // during an incremental commit, a "terminate" call can be issued on the
        // same timestamp as an "insert" call. This must result in the deletion
        // of the entry (instead of its regular termination).
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "name", "default", 314, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "name", "default", 512, "2222", 1000)
            SecondaryLongIndexStore.terminateValidity(tx, "name", "default", 512, "2222", 1000, 0L)
            SecondaryLongIndexStore.insert(tx, "name", "default", 654, "3333", 1000)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryLongIndexStore.storeName("name", "default")) }
        indexEntries shouldBe listOf(
                Triple(314L, "1111", listOf(1000L, Long.MAX_VALUE)),
                Triple(654L, "3333", listOf(1000L, Long.MAX_VALUE))
        )
    }

    @Test
    fun terminationOfEntryFromDifferentBranchWorks() {
        // when terminating an entry, it can happen that the termination is
        // the first update to this entry on this branch, i.e. there is no
        // entry to terminate. In this case, the algorithm inserts a new
        // entry with the previous value from zero and terminates it at the
        // termination timestamp to "simulate" the termination.
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "name", "default", 314, "1111", 1000)
            SecondaryLongIndexStore.terminateValidity(tx, "name", "default", 512, "2222", 1000, 0L)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryLongIndexStore.storeName("name", "default")) }
        indexEntries shouldBe listOf(
                // regular entry
                Triple(314L, "1111", listOf(1000L, Long.MAX_VALUE)),
                // "simulated" termination
                Triple(512L, "2222", listOf(0L, 1000L))
        )
    }

    @Test
    fun canSeparateKeyspaces() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "alpha", 314, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "alpha", 314, "1112", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "beta", 512, "2222", 1000)
            SecondaryLongIndexStore.terminateValidity(tx, "value", "beta", 512, "2222", 2000, 0L)
            SecondaryLongIndexStore.insert(tx, "value", "beta", 512, "2223", 3000)
            tx.commit()
        }
        val entriesAlpha = this.readOnlyTx { tx -> readEntries(tx, SecondaryLongIndexStore.storeName("value", "alpha")) }
        entriesAlpha shouldBe listOf(
            Triple(314L, "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple(314L, "1112", listOf(1000L, Long.MAX_VALUE))
        )
        val entriesBeta = this.readOnlyTx { tx -> readEntries(tx, SecondaryLongIndexStore.storeName("value", "beta")) }
        entriesBeta shouldBe listOf(
            Triple(512L, "2222", listOf(1000L, 2000)),
            Triple(512L, "2223", listOf(3000L, Long.MAX_VALUE))
        )

        val spec1 = LongSearchSpecification.create("value", Condition.EQUALS, 314)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, spec1, "alpha", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(314L, "1111"),
                    ScanResultEntry(314L, "1112")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
            // doing the same query on the beta keyspace shouldn't yield any results
            SecondaryLongIndexStore.scan(tx, spec1, "beta", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }

        val spec2 = LongSearchSpecification.create("value", Condition.EQUALS, 512)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, spec2, "beta", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(512L, "2222")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
            // doing the same query on the alpha keyspace shouldn't yield any results
            SecondaryLongIndexStore.scan(tx, spec2, "alpha", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canQueryEmptyStore() {
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.EQUALS, 314)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "youDoNotExist", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateEquals() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 300, "7487", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 314, "1112", 1000)
            SecondaryLongIndexStore.terminateValidity(tx, "value", "default", 314, "1112", 2000, 0L)
            SecondaryLongIndexStore.insert(tx, "value", "default", 675, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 675, "2223", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 115235, "5555", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 568275864, "6666", 1000)
            tx.commit()
        }
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.EQUALS, 314)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(314L, "1111")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateGreaterThan() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 10, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 11, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 12, "3333", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 13, "4444", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 14, "5555", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 15, "6666", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 16, "7777", 1000)
            tx.commit()
        }
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.GREATER_THAN, 13)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(14L, "5555"),
                    ScanResultEntry(15L, "6666"),
                    ScanResultEntry(16L, "7777")

                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateGreaterOrEqual() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 10, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 11, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 12, "3333", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 13, "4444", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 14, "5555", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 15, "6666", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 16, "7777", 1000)
            tx.commit()
        }
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.GREATER_EQUAL, 13)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(13L, "4444"),
                    ScanResultEntry(14L, "5555"),
                    ScanResultEntry(15L, "6666"),
                    ScanResultEntry(16L, "7777")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateLessThan() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 10, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 11, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 12, "3333", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 13, "4444", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 14, "5555", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 15, "6666", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 16, "7777", 1000)
            tx.commit()
        }
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.LESS_THAN, 13)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(12L, "3333"),
                    ScanResultEntry(11L, "2222"),
                    ScanResultEntry(10L, "1111")
                ),
                OrderedBy("value", Order.DESCENDING)
            )
        }
    }

    @Test
    fun canEvaluateLessOrEqual() {
        this.readWriteTx { tx ->
            SecondaryLongIndexStore.insert(tx, "value", "default", 10, "1111", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 11, "2222", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 12, "3333", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 13, "4444", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 14, "5555", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 15, "6666", 1000)
            SecondaryLongIndexStore.insert(tx, "value", "default", 16, "7777", 1000)
            tx.commit()
        }
        val searchSpec = LongSearchSpecificationImpl("value", NumberCondition.LESS_EQUAL, 13)
        this.readOnlyTx { tx ->
            SecondaryLongIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(13L, "4444"),
                    ScanResultEntry(12L, "3333"),
                    ScanResultEntry(11L, "2222"),
                    ScanResultEntry(10L, "1111")
                ),
                OrderedBy("value", Order.DESCENDING)
            )
        }
    }


    @Test
    fun canCreateAndParseSecondaryIndexKeys() {
        val john1234 = SecondaryLongIndexStore.createSecondaryIndexKey(314, "p-1234")
        val poop4567 = SecondaryLongIndexStore.createSecondaryIndexKey(675, "p-4567")
        SecondaryLongIndexStore.parseSecondaryIndexKey(john1234).toScanResultEntry() shouldBe ScanResultEntry(314L, "p-1234")
        SecondaryLongIndexStore.parseSecondaryIndexKey(poop4567).toScanResultEntry() shouldBe ScanResultEntry(675L, "p-4567")
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun <T> readWriteTx(action: (ExodusTransaction) -> T): T {
        return ExodusTransactionImpl(this.environment, this.environment.beginExclusiveTransaction()).use(action)
    }

    private fun <T> readOnlyTx(action: (ExodusTransaction) -> T): T {
        return ExodusTransactionImpl(this.environment, this.environment.beginReadonlyTransaction()).use(action)
    }

    private fun readKeys(tx: ExodusTransaction, storeName: String): List<ScanResultEntry<Long>> {
        val resultList = mutableListOf<ScanResultEntry<Long>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                resultList.add(SecondaryLongIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry())
            }
        }
        return resultList
    }

    private fun readEntries(tx: ExodusTransaction, storeName: String): List<Triple<Long, String, List<Long>>> {
        val resultList = mutableListOf<Triple<Long, String, List<Long>>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                val (indexValue, userKey) = SecondaryLongIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry()
                val timestamps = readLongsFromBytes(cursor.value.toByteArray())
                resultList.add(Triple(indexValue, userKey, timestamps))
            }
        }
        return resultList
    }
}