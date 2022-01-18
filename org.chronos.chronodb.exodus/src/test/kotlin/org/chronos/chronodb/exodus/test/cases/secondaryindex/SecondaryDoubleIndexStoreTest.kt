package org.chronos.chronodb.exodus.test.cases.secondaryindex

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.NumberCondition
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.secondaryindex.stores.OrderedBy
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanResult
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanResultEntry
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryDoubleIndexStore
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.transaction.ExodusTransactionImpl
import org.chronos.chronodb.exodus.util.readLongsFromBytes
import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.query.DoubleSearchSpecificationImpl
import org.chronos.common.test.utils.NamedPayload
import org.junit.jupiter.api.Test

class SecondaryDoubleIndexStoreTest : EnvironmentTest(){

    @Test
    fun canInsertIntoStore() {
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1112", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1113", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 5.75, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 6.25, "3333", 2000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 7.36, "4444", 2000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexKeys = this.readOnlyTx { tx -> readKeys(tx, SecondaryDoubleIndexStore.storeName("value", "default")) }

        // check that the contents are correct
        indexKeys shouldBe listOf(
                ScanResultEntry(3.1415, "1111"),
                ScanResultEntry(3.1415, "1112"),
                ScanResultEntry(3.1415, "1113"),
                ScanResultEntry(5.75, "2222"),
                ScanResultEntry(6.25, "3333"),
                ScanResultEntry(7.36, "4444")
        )
    }

    @Test
    fun canTerminateValidityPeriods() {
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1112", 1000)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 4.25, "2222", 1000)
            SecondaryDoubleIndexStore.terminateValidity(tx, "value", "default", 3.1415, "1112", 2000, 0L)
            SecondaryDoubleIndexStore.insert(tx, "value", "default", 3.1415, "1112", 3000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryDoubleIndexStore.storeName("value", "default")) }
        indexEntries shouldBe listOf(
                Triple(3.1415, "1111", listOf(1000L, Long.MAX_VALUE)),
                Triple(3.1415, "1112", listOf(1000L, 2000L, 3000L, Long.MAX_VALUE)),
                Triple(4.25, "2222", listOf(1000L, Long.MAX_VALUE))
        )
    }

    @Test
    fun terminationDuringIncrementalCommitsWorks() {
        // during an incremental commit, a "terminate" call can be issued on the
        // same timestamp as an "insert" call. This must result in the deletion
        // of the entry (instead of its regular termination).
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, "name", "default", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, "name", "default", 5.12, "2222", 1000)
            SecondaryDoubleIndexStore.terminateValidity(tx, "name", "default", 5.12, "2222", 1000, 0L)
            SecondaryDoubleIndexStore.insert(tx, "name", "default", 6.54, "3333", 1000)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryDoubleIndexStore.storeName("name", "default")) }
        indexEntries shouldBe listOf(
                Triple(3.1415, "1111", listOf(1000L, Long.MAX_VALUE)),
                Triple(6.54, "3333", listOf(1000L, Long.MAX_VALUE))
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
            SecondaryDoubleIndexStore.insert(tx, "name", "default", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.terminateValidity(tx, "name", "default", 5.12, "2222", 1000, 0L)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryDoubleIndexStore.storeName("name", "default")) }
        indexEntries shouldBe listOf(
                // regular entry
                Triple(3.1415, "1111", listOf(1000L, Long.MAX_VALUE)),
                // "simulated" termination
                Triple(5.12, "2222", listOf(0L, 1000L))
        )
    }


    @Test
    fun canSeparateKeyspaces() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "alpha", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "alpha", 3.1415, "1112", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "beta", 5.12, "2222", 1000)
            SecondaryDoubleIndexStore.terminateValidity(tx, indexId, "beta", 5.12, "2222", 2000, 0L)
            SecondaryDoubleIndexStore.insert(tx, indexId, "beta", 5.12, "2223", 3000)
            tx.commit()
        }
        val entriesAlpha = this.readOnlyTx { tx -> readEntries(tx, SecondaryDoubleIndexStore.storeName(indexId, "alpha")) }
        entriesAlpha shouldBe listOf(
            Triple(3.1415, "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple(3.1415, "1112", listOf(1000L, Long.MAX_VALUE))
        )
        val entriesBeta = this.readOnlyTx { tx -> readEntries(tx, SecondaryDoubleIndexStore.storeName(indexId, "beta")) }
        entriesBeta shouldBe listOf(
            Triple(5.12, "2222", listOf(1000L, 2000)),
            Triple(5.12, "2223", listOf(3000L, Long.MAX_VALUE))
        )

        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val spec1 = DoubleSearchSpecification.create(index, Condition.EQUALS, 3.1415, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, spec1, "alpha", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(3.1415, "1111"),
                    ScanResultEntry(3.1415, "1112")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
            // doing the same query on the beta keyspace shouldn't yield any results
            SecondaryDoubleIndexStore.scan(tx, spec1, "beta", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }

        val spec2 = DoubleSearchSpecification.create(index, Condition.EQUALS, 5.12, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, spec2, "beta", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(5.12, "2222")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
            // doing the same query on the alpha keyspace shouldn't yield any results
            SecondaryDoubleIndexStore.scan(tx, spec2, "alpha", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canQueryEmptyStore(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.EQUALS, 3.1415, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "youDoNotExist", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateEquals() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 3.0, "7487", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 3.1415, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 3.1415, "1112", 1000)
            SecondaryDoubleIndexStore.terminateValidity(tx, indexId, "default", 3.1415, "1112", 2000, 0L)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 6.75, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 6.75, "2223", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1152.35, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 56827.5864, "6666", 1000)
            tx.commit()
        }


        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.EQUALS, 3.1415, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(3.1415, "1111")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateEqualsWithTolerance(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.0, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.1, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.2, "3333", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.3, "4444", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.4, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.5, "6666", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.6, "7777", 1000)
            tx.commit()
        }

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.EQUALS, 1.3, 0.2)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(1.1, "2222"),
                    ScanResultEntry(1.2, "3333"),
                    ScanResultEntry(1.3, "4444"),
                    ScanResultEntry(1.4, "5555"),
                    ScanResultEntry(1.5, "6666")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateGreaterThan(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.0, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.1, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.2, "3333", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.3, "4444", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.4, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.5, "6666", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.6, "7777", 1000)
            tx.commit()
        }

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.GREATER_THAN, 1.3, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(1.4, "5555"),
                    ScanResultEntry(1.5, "6666"),
                    ScanResultEntry(1.6, "7777")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateGreaterOrEqual(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.0, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.1, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.2, "3333", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.3, "4444", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.4, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.5, "6666", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.6, "7777", 1000)
            tx.commit()
        }

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.GREATER_EQUAL, 1.3, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(1.3, "4444"),
                    ScanResultEntry(1.4, "5555"),
                    ScanResultEntry(1.5, "6666"),
                    ScanResultEntry(1.6, "7777")
                ),
                OrderedBy("value", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateLessThan(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.0, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.1, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.2, "3333", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.3, "4444", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.4, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.5, "6666", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.6, "7777", 1000)
            tx.commit()
        }

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.LESS_THAN, 1.3, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(1.2, "3333"),
                    ScanResultEntry(1.1, "2222"),
                    ScanResultEntry(1.0, "1111")
                ),
                OrderedBy("value", Order.DESCENDING)
            )
        }
    }

    @Test
    fun canEvaluateLessOrEqual(){
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "value",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        this.readWriteTx { tx ->
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.0, "1111", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.1, "2222", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.2, "3333", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.3, "4444", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.4, "5555", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.5, "6666", 1000)
            SecondaryDoubleIndexStore.insert(tx, indexId, "default", 1.6, "7777", 1000)
            tx.commit()
        }

        val searchSpec = DoubleSearchSpecificationImpl(index, NumberCondition.LESS_EQUAL, 1.30, 0.001)
        this.readOnlyTx { tx ->
            SecondaryDoubleIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry(1.3, "4444"),
                    ScanResultEntry(1.2, "3333"),
                    ScanResultEntry(1.1, "2222"),
                    ScanResultEntry(1.0, "1111")
                ),
                OrderedBy("value", Order.DESCENDING)
            )
        }
    }



    @Test
    fun canCreateAndParseSecondaryIndexKeys() {
        val john1234 = SecondaryDoubleIndexStore.createSecondaryIndexKey(3.1415, "p-1234")
        val poop4567 = SecondaryDoubleIndexStore.createSecondaryIndexKey(6.75, "p-4567")
        SecondaryDoubleIndexStore.parseSecondaryIndexKey(john1234).toScanResultEntry() shouldBe ScanResultEntry(3.1415, "p-1234")
        SecondaryDoubleIndexStore.parseSecondaryIndexKey(poop4567).toScanResultEntry() shouldBe ScanResultEntry(6.75, "p-4567")
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

    private fun readKeys(tx: ExodusTransaction, storeName: String): List<ScanResultEntry<Double>> {
        val resultList = mutableListOf<ScanResultEntry<Double>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                resultList.add(SecondaryDoubleIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry())
            }
        }
        return resultList
    }

    private fun readEntries(tx: ExodusTransaction, storeName: String): List<Triple<Double, String, List<Long>>> {
        val resultList = mutableListOf<Triple<Double, String, List<Long>>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                val (indexValue, userKey) = SecondaryDoubleIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry()
                val timestamps = readLongsFromBytes(cursor.value.toByteArray())
                resultList.add(Triple(indexValue, userKey, timestamps))
            }
        }
        return resultList
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class DummyIndexer: DoubleIndexer {

        override fun canIndex(`object`: Any?): Boolean {
            return true
        }

        override fun getIndexValues(`object`: Any?): Set<Double> {
            return emptySet()
        }

    }
}