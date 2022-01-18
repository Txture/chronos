package org.chronos.chronodb.test.cases.engine.keyspace

import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(IntegrationTest::class)
class KeySetTest : AllChronoDBBackendsTest() {

    @Test
    fun canQueryKeySetOnBranch() {
        val db = this.chronoDB

        val commit1 = db.tx().let { tx ->
            tx.put("vertex", "v1", 1)
            tx.put("vertex", "v2", 2)
            tx.commit()
        }

        val commit2 = db.tx().let { tx ->
            tx.put("vertex", "v3", 3)
            tx.put("vertex", "v4", 4)
            tx.commit()
        }

        db.branchManager.createBranch("MyBranch", commit2)

        db.tx("MyBranch").let { tx ->
            tx.put("vertex", "v100", 100)
            tx.remove("vertex", "v1")
            tx.commit()
        }

        db.tx().let { tx ->
            tx.put("vertex", "v42", 42)
            tx.remove("vertex", "v2")
            tx.commit()
        }

        val keySetBeforeBranchingTimestamp = db.tx("MyBranch", commit1).keySet("vertex")
        assertEquals(setOf("v1", "v2"), keySetBeforeBranchingTimestamp)

        val keySetOnBranchHead = db.tx("MyBranch").keySet("vertex")
        assertEquals(setOf("v2", "v3", "v4", "v100"), keySetOnBranchHead)

        val keySetOnMasterHead = db.tx().keySet("vertex")
        assertEquals(setOf("v1", "v3", "v4", "v42"), keySetOnMasterHead)
    }

}