package org.chronos.chronodb.exodus.test.cases.secondaryindex

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.Order
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.query.Condition
import org.chronos.chronodb.api.query.StringCondition
import org.chronos.chronodb.exodus.kotlin.ext.toByteArray
import org.chronos.chronodb.exodus.secondaryindex.stores.OrderedBy
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanResult
import org.chronos.chronodb.exodus.secondaryindex.stores.ScanResultEntry
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryStringIndexStore
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.transaction.ExodusTransactionImpl
import org.chronos.chronodb.exodus.util.readLongsFromBytes
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.query.StringSearchSpecificationImpl
import org.chronos.chronodb.internal.impl.query.TextMatchMode
import org.chronos.chronodb.internal.impl.query.TextMatchMode.CASE_INSENSITIVE
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.junit.jupiter.api.Test

class SecondaryStringIndexStoreTest : EnvironmentTest() {

    @Test
    fun canInsertIntoStore() {
        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1112", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1113", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Adam", "2222", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Jack", "3333", 2000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Jane", "4444", 2000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexKeys = this.readOnlyTx { tx -> readKeys(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        val indexKeysCI = this.readOnlyTx { tx -> readKeysCI(tx, SecondaryStringIndexStore.storeNameCI("name", "default")) }

        // check that the contents are correct
        indexKeys shouldBe listOf(
            ScanResultEntry("Adam", "2222"),
            ScanResultEntry("Jack", "3333"),
            ScanResultEntry("Jane", "4444"),
            ScanResultEntry("John", "1111"),
            ScanResultEntry("John", "1112"),
            ScanResultEntry("John", "1113")
        )
        indexKeysCI shouldBe listOf(
            ScanResultEntry("adam", "2222"),
            ScanResultEntry("jack", "3333"),
            ScanResultEntry("jane", "4444"),
            ScanResultEntry("john", "1111"),
            ScanResultEntry("john", "1112"),
            ScanResultEntry("john", "1113")
        )
    }

    @Test
    fun canTerminateValidityPeriods() {
        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1112", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Jane", "2222", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "John", "1112", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1112", 3000)
            tx.commit()
        }
        // read the contents of the store (manually)
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        val indexEntriesCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI("name", "default")) }
        indexEntries shouldBe listOf(
            Triple("Jane", "2222", listOf(1000L, Long.MAX_VALUE)),
            Triple("John", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("John", "1112", listOf(1000L, 2000L, 3000L, Long.MAX_VALUE))
        )
        indexEntriesCI shouldBe listOf(
            Triple("jane", "2222", listOf(1000L, Long.MAX_VALUE)),
            Triple("john", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("john", "1112", listOf(1000L, 2000L, 3000L, Long.MAX_VALUE))
        )
    }

    @Test
    fun terminationDuringIncrementalCommitsWorks() {
        // during an incremental commit, a "terminate" call can be issued on the
        // same timestamp as an "insert" call. This must result in the deletion
        // of the entry (instead of its regular termination).
        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Jane", "2222", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Jane", "2222", 1000, 0L)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Jack", "3333", 1000)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        val indexEntriesCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI("name", "default")) }
        indexEntries shouldBe listOf(
            Triple("Jack", "3333", listOf(1000L, Long.MAX_VALUE)),
            Triple("John", "1111", listOf(1000L, Long.MAX_VALUE))
        )
        indexEntriesCI shouldBe listOf(
            Triple("jack", "3333", listOf(1000L, Long.MAX_VALUE)),
            Triple("john", "1111", listOf(1000L, Long.MAX_VALUE))
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
            SecondaryStringIndexStore.insert(tx, "name", "default", "John", "1111", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Jane", "2222", 1000, 0L)
            tx.commit()
        }
        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        val indexEntriesCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI("name", "default")) }
        indexEntries shouldBe listOf(
            // "simulated" termination
            Triple("Jane", "2222", listOf(0L, 1000L)),
            // regular entry
            Triple("John", "1111", listOf(1000L, Long.MAX_VALUE))
        )
        indexEntriesCI shouldBe listOf(
            Triple("jane", "2222", listOf(0L, 1000L)),
            Triple("john", "1111", listOf(1000L, Long.MAX_VALUE))
        )
    }

    @Test
    fun canSeparateKeyspaces() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "name",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, indexId, "male", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "male", "John", "1112", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "female", "Jane", "2222", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "female", "Jane", "2222", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, indexId, "female", "Jane", "2223", 3000)
            tx.commit()
        }
        val entriesMale = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName(indexId, "male")) }
        entriesMale shouldBe listOf(
            Triple("John", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("John", "1112", listOf(1000L, Long.MAX_VALUE))
        )
        val entriesMaleCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI(indexId, "male")) }
        entriesMaleCI shouldBe listOf(
            Triple("john", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("john", "1112", listOf(1000L, Long.MAX_VALUE))
        )

        val entriesFemale = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName(indexId, "female")) }
        entriesFemale shouldBe listOf(
            Triple("Jane", "2222", listOf(1000L, 2000)),
            Triple("Jane", "2223", listOf(3000L, Long.MAX_VALUE))
        )
        val entriesFemaleCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI(indexId, "female")) }
        entriesFemaleCI shouldBe listOf(
            Triple("jane", "2222", listOf(1000L, 2000)),
            Triple("jane", "2223", listOf(3000L, Long.MAX_VALUE))
        )

        val johnsSpec = StringSearchSpecification.create(index, Condition.EQUALS, TextMatchMode.STRICT, "John")
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, johnsSpec, "male", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("John", "1111"),
                    ScanResultEntry("John", "1112")
                ),
                OrderedBy("name", Order.ASCENDING)
            )
            // doing the same query on the female keyspace shouldn't yield any results
            SecondaryStringIndexStore.scan(tx, johnsSpec, "female", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("name", Order.ASCENDING)
            )
        }

        val janesSpec = StringSearchSpecification.create(index, Condition.EQUALS, TextMatchMode.STRICT, "Jane")
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, janesSpec, "female", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("Jane", "2222")
                ),
                OrderedBy("name", Order.ASCENDING)
            )
            // doing the same query on the male keyspace shouldn't yield any results
            SecondaryStringIndexStore.scan(tx, janesSpec, "male", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("name", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canHandleClashesInCaseInsensitiveIndex() {
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
            // note that all three share the same primary key.
            // This happens if there is a multi-value being indexed. This multi-value
            // contains: "John", "john" and "Jack".
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "john", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "jack", "1111", 1000)
            // terminate the validity of "John" (but not "john"!)
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "default", "John", "1111", 2000, 0L)
            // later, terminate the validity of "john" too
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "default", "john", "1111", 3000, 0L)
            tx.commit()
        }
        val spec = StringSearchSpecification.create(index, Condition.EQUALS, CASE_INSENSITIVE, "John")
        this.readOnlyTx { tx ->
            // scan at T = 1000 should deliver "john" only once even though both "john" and "John" match (clash)
            SecondaryStringIndexStore.scan(tx, spec, "default", 1000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("john", "1111")
                )
            )
            // scan at T = 2000 (after termination of "John") should still produce "john"
            SecondaryStringIndexStore.scan(tx, spec, "default", 2000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("john", "1111")
                )
            )
            // scan at T = 3000 (after termination of "john") should produce no results
            SecondaryStringIndexStore.scan(tx, spec, "default", 3000) shouldBe ScanResult(
                listOf()
            )
        }

    }

    @Test
    fun canQueryEmptyStore() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "name",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val spec = StringSearchSpecification.create(index, Condition.EQUALS, TextMatchMode.STRICT, "Jane")
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, spec, "youDoNotExist", 1000) shouldBe ScanResult(
                listOf(),
                OrderedBy("name", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateStartsWith() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "name",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )


        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, indexId, "default", "jay", "7487", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1112", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "default", "John", "1112", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2222", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2223", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Billy", "5555", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Adam", "6666", 1000)
            tx.commit()
        }
        val searchSpec = StringSearchSpecificationImpl(index, StringCondition.STARTS_WITH, "j", TextMatchMode.STRICT)
        val searchSpecCI = StringSearchSpecificationImpl(index, StringCondition.STARTS_WITH, "j", TextMatchMode.CASE_INSENSITIVE)
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, searchSpecCI, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("jane", "2222"),
                    ScanResultEntry("jane", "2223"),
                    ScanResultEntry("jay", "7487"),
                    ScanResultEntry("john", "1111")
                )
            )
            SecondaryStringIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("jay", "7487")
                ),
                OrderedBy("name", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateEquals() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "name",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )


        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, indexId, "default", "john", "7487", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1112", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "default", "John", "1112", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2222", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2223", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Billy", "5555", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Adam", "6666", 1000)
            tx.commit()
        }
        val searchSpec = StringSearchSpecificationImpl(index, StringCondition.EQUALS, "John", TextMatchMode.STRICT)
        val searchSpecCI = StringSearchSpecificationImpl(index, StringCondition.EQUALS, "John", TextMatchMode.CASE_INSENSITIVE)
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, searchSpecCI, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("john", "1111"),
                    ScanResultEntry("john", "7487")
                )
            )
            SecondaryStringIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("John", "1111")
                ),
                OrderedBy("name", Order.ASCENDING)
            )
        }
    }

    @Test
    fun canEvaluateRegexQuery() {
        val indexId = "dd1f3a91-12d5-47bd-a343-7b15a3475f09"
        val index = SecondaryIndexImpl(
            id = indexId,
            name = "name",
            indexer = DummyIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        this.readWriteTx { tx ->
            SecondaryStringIndexStore.insert(tx, indexId, "default", "john", "7487", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1111", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "John", "1112", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, indexId, "default", "John", "1112", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2222", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Jane", "2223", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Billy", "5555", 1000)
            SecondaryStringIndexStore.insert(tx, indexId, "default", "Adam", "6666", 1000)
            tx.commit()
        }
        val searchSpec = StringSearchSpecificationImpl(index, StringCondition.MATCHES_REGEX, "J.*n.*", TextMatchMode.STRICT)
        val searchSpecCI = StringSearchSpecificationImpl(index, StringCondition.MATCHES_REGEX, "J.*n.*", TextMatchMode.CASE_INSENSITIVE)
        this.readOnlyTx { tx ->
            SecondaryStringIndexStore.scan(tx, searchSpec, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("Jane", "2222"),
                    ScanResultEntry("Jane", "2223"),
                    ScanResultEntry("John", "1111")
                ),
                OrderedBy("name", Order.ASCENDING)
            )
            SecondaryStringIndexStore.scan(tx, searchSpecCI, "default", 3000) shouldBe ScanResult(
                listOf(
                    ScanResultEntry("jane", "2222"),
                    ScanResultEntry("jane", "2223"),
                    ScanResultEntry("john", "1111"),
                    ScanResultEntry("john", "7487")
                )
            )
        }
    }

    @Test
    fun canCreateAndParseSecondaryIndexKeys() {
        val john1234 = SecondaryStringIndexStore.createSecondaryIndexKey("John", "p-1234")
        val poop4567 = SecondaryStringIndexStore.createSecondaryIndexKey("\uD83D\uDCA9", "p-4567")
        SecondaryStringIndexStore.parseSecondaryIndexKey(john1234).toScanResultEntry() shouldBe ScanResultEntry("John", "p-1234")
        SecondaryStringIndexStore.parseSecondaryIndexKey(poop4567).toScanResultEntry() shouldBe ScanResultEntry("\uD83D\uDCA9", "p-4567")
    }

    @Test
    fun canPerformRollback() {
        this.readWriteTx { tx ->
            // this "doe" was renamed to "smith" at 2000
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "1111", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Doe", "1111", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Smith", "1111", 2000)

            // this "doe" ceased to exist at 2000
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "2222", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Doe", "2222", 2000, 0L)

            // this "doe" cased to exist at 2000, re-appeared at 3000, disappeared at 4000 and re-appeared at 5000
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "3333", 1000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Doe", "3333", 2000, 0L)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "3333", 3000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Doe", "3333", 4000, 0L)
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "3333", 5000)

            // this "doe" was inserted at 1000 and stayed forever
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "4444", 1000)

            // this "doe" was inserted at 3000 and stayed forever
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "5555", 3000)

            // this "doe" was inserted at 3000 and disappeared at 4000
            SecondaryStringIndexStore.insert(tx, "name", "default", "Doe", "6666", 3000)
            SecondaryStringIndexStore.terminateValidity(tx, "name", "default", "Doe", "6666", 4000, 0L)

            tx.commit()
        }

        val indexEntries = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        // make sure the entries are correct
        indexEntries shouldBe listOf(
            Triple("Doe", "1111", listOf(1000L, 2000L)),
            Triple("Doe", "2222", listOf(1000L, 2000L)),
            Triple("Doe", "3333", listOf(1000L, 2000L, 3000L, 4000L, 5000L, Long.MAX_VALUE)),
            Triple("Doe", "4444", listOf(1000L, Long.MAX_VALUE)),
            Triple("Doe", "5555", listOf(3000L, Long.MAX_VALUE)),
            Triple("Doe", "6666", listOf(3000L, 4000L)),
            Triple("Smith", "1111", listOf(2000, Long.MAX_VALUE))
        )

        // perform a rollback to T = 2000
        this.readWriteTx { tx ->
            SecondaryStringIndexStore.rollback(tx, "name", 2000)
            tx.commit()
        }

        // check the index contents again
        val indexEntries2 = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        indexEntries2 shouldBe listOf(
            Triple("Doe", "1111", listOf(1000L, 2000L)),
            Triple("Doe", "2222", listOf(1000L, 2000L)),
            Triple("Doe", "3333", listOf(1000L, 2000L)),
            Triple("Doe", "4444", listOf(1000L, Long.MAX_VALUE)),
            Triple("Smith", "1111", listOf(2000L, Long.MAX_VALUE))
        )

        // perform a rollback to T = 1500
        this.readWriteTx { tx ->
            SecondaryStringIndexStore.rollback(tx, "name", 1500)
            tx.commit()
        }

        // check the index contents again
        val indexEntries3 = this.readOnlyTx { tx -> readEntries(tx, SecondaryStringIndexStore.storeName("name", "default")) }
        indexEntries3 shouldBe listOf(
            Triple("Doe", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("Doe", "2222", listOf(1000L, Long.MAX_VALUE)),
            Triple("Doe", "3333", listOf(1000L, Long.MAX_VALUE)),
            Triple("Doe", "4444", listOf(1000L, Long.MAX_VALUE))
        )

        // all of this should also affect our CI index
        val indexEntriesCI = this.readOnlyTx { tx -> readEntriesCI(tx, SecondaryStringIndexStore.storeNameCI("name", "default")) }
        indexEntriesCI shouldBe listOf(
            Triple("doe", "1111", listOf(1000L, Long.MAX_VALUE)),
            Triple("doe", "2222", listOf(1000L, Long.MAX_VALUE)),
            Triple("doe", "3333", listOf(1000L, Long.MAX_VALUE)),
            Triple("doe", "4444", listOf(1000L, Long.MAX_VALUE))
        )

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

    private fun readKeys(tx: ExodusTransaction, storeName: String): List<ScanResultEntry<String>> {
        val resultList = mutableListOf<ScanResultEntry<String>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                resultList.add(SecondaryStringIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry())
            }
        }
        return resultList
    }

    private fun readKeysCI(tx: ExodusTransaction, storeName: String): List<ScanResultEntry<String>> {
        val resultList = mutableListOf<ScanResultEntry<String>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                resultList.add(SecondaryStringIndexStore.parseSecondaryIndexKeyCI(cursor.key).toScanResultEntry())
            }
        }
        return resultList
    }


    private fun readEntries(tx: ExodusTransaction, storeName: String): List<Triple<String, String, List<Long>>> {
        val resultList = mutableListOf<Triple<String, String, List<Long>>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                val (indexValue, userKey) = SecondaryStringIndexStore.parseSecondaryIndexKey(cursor.key).toScanResultEntry()
                val timestamps = readLongsFromBytes(cursor.value.toByteArray())
                resultList.add(Triple(indexValue, userKey, timestamps))
            }
        }
        return resultList
    }

    private fun readEntriesCI(tx: ExodusTransaction, storeName: String): List<Triple<String, String, List<Long>>> {
        val resultList = mutableListOf<Triple<String, String, List<Long>>>()
        tx.openCursorOn(storeName).use { cursor ->
            while (cursor.next) {
                val (indexValue, userKey) = SecondaryStringIndexStore.parseSecondaryIndexKeyCI(cursor.key).toScanResultEntry()
                val timestamps = readLongsFromBytes(cursor.value.toByteArray())
                resultList.add(Triple(indexValue, userKey, timestamps))
            }
        }
        return resultList
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class DummyIndexer : StringIndexer {

        override fun canIndex(`object`: Any?): Boolean {
            return true
        }

        override fun getIndexValues(`object`: Any?): Set<String> {
            return emptySet()
        }

    }

}