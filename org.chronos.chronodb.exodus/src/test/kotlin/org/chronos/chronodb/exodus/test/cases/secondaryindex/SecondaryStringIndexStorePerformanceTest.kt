package org.chronos.chronodb.exodus.test.cases.secondaryindex

import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.api.query.StringCondition
import org.chronos.chronodb.exodus.secondaryindex.stores.SecondaryStringIndexStore
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.chronodb.exodus.transaction.ExodusTransaction
import org.chronos.chronodb.exodus.transaction.ExodusTransactionImpl
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.query.TextMatchMode
import org.chronos.common.test.utils.model.person.PersonGenerator
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.system.measureTimeMillis

class SecondaryStringIndexStorePerformanceTest : EnvironmentTest() {

    @Test
    @Tag("performance")
    fun executePersonPerformanceTest() {
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

        val personCount = 1_000_000
        println("Generating ${personCount} persons...")
        val persons = PersonGenerator.generateRandomPersons(personCount)
        println("Generated ${personCount} persons.")
        println("Starting insert into secondary index...")
        measureTimeMillis {
            this.readWriteTx { tx ->
                persons.forEach { person ->
                    SecondaryStringIndexStore.insert(tx, indexId, "${person.firstName} ${person.lastName}", UUID.randomUUID().toString(), "default", 1000)
                }
                tx.commit()
            }
        }.let { println("Secondary Index built in ${it}ms. Size on disk: ${FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(this.testDir))}") }

        measureTimeMillis {
            this.readOnlyTx { tx ->
                val jackSmiths = SecondaryStringIndexStore.scan(tx, StringSearchSpecification.create(index, StringCondition.EQUALS, TextMatchMode.CASE_INSENSITIVE, "Jack Smith"), "default", 1000)
                println("Found ${jackSmiths.size} 'Jack Smith's (equals, case insensitive).")
            }
        }.let { println("Query time: ${it}ms.") }
        measureTimeMillis {
            this.readOnlyTx { tx ->
                val sams = SecondaryStringIndexStore.scan(tx, StringSearchSpecification.create(index, StringCondition.CONTAINS, TextMatchMode.CASE_INSENSITIVE, "Sam"), "default", 1000)
                println("Found ${sams.size} 'Sam's (contains, case insensitive).")
            }
        }.let { println("Query time: ${it}ms.") }
        measureTimeMillis {
            this.readOnlyTx { tx ->
                val jos = SecondaryStringIndexStore.scan(tx, StringSearchSpecification.create(index, StringCondition.STARTS_WITH, TextMatchMode.CASE_INSENSITIVE, "Jo"), "default", 1000)
                println("Found ${jos.size} 'Jo's (starts with, case insensitive).")
            }
        }.let{ println("Query time: ${it}ms.")}
    }

    private fun <T> readWriteTx(action: (ExodusTransaction) -> T): T {
        return ExodusTransactionImpl(this.environment, this.environment.beginExclusiveTransaction()).use(action)
    }

    private fun <T> readOnlyTx(action: (ExodusTransaction) -> T): T {
        return ExodusTransactionImpl(this.environment, this.environment.beginReadonlyTransaction()).use(action)
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class DummyIndexer: StringIndexer {

        override fun canIndex(`object`: Any?): Boolean {
            return true
        }

        override fun getIndexValues(`object`: Any?): Set<String> {
            return emptySet()
        }

    }

}