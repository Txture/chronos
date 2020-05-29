package org.chronos.chronodb.test.cases

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.key.QualifiedKey
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.hamcrest.Matchers.*
import org.junit.Assert.assertThat
import org.junit.Test
import org.junit.experimental.categories.Category
import java.util.concurrent.TimeUnit

@Category(IntegrationTest::class)
class DatebackKeyspaceMetadataTest : AllChronoDBBackendsTest() {

    @Test
    fun injectingIntoThePastUpdatesKeyspaceCreationTimestamp() {
        val db = this.chronoDB

        // insert some data
        val tx1 = db.tx()
        tx1.put("hello", "world")
        tx1.put("math", "pi", 3.1415)
        tx1.put("persons", "john", "doe")
        val firstCommit = tx1.commit()

        // assert the keyspaces have been created
        assertThat(db.tx().timestamp, `is`(firstCommit))
        assertThat(db.tx().keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "math", "persons"))
        // the commit before should only contain the default keyspace (which always exists)
        assertThat(db.tx(firstCommit-1).keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME))

        // perform a dateback insertion 24 hours earlier
        val oneDayEarlier = firstCommit - TimeUnit.HOURS.toMillis(24)

        db.datebackManager.datebackOnMaster{ dateback ->
            val entries = mutableMapOf<QualifiedKey, Any>()
            entries[QualifiedKey.create("newkeyspace", "foo")] = "bar"
            entries[QualifiedKey.create("default", "foo")] = "baz"
            entries[QualifiedKey.create("math", "epsilon")] = 2.7182
            dateback.inject(oneDayEarlier, entries)
        }

        // assert the keyspace creation dates have been updated
        assertThat(db.tx().timestamp, `is`(firstCommit))
        assertThat(db.tx().keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "math", "persons", "newkeyspace"))
        assertThat(db.tx(firstCommit-1).keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "math", "newkeyspace"))
        assertThat(db.tx(oneDayEarlier).keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "math", "newkeyspace"))
        // the commit before should only contain the default keyspace (which always exists)
        assertThat(db.tx(oneDayEarlier-1).keyspaces(), containsInAnyOrder(ChronoDBConstants.DEFAULT_KEYSPACE_NAME))

    }

}