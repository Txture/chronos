package org.chronos.chronodb.test.cases.engine.branching

import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.junit.Assert.assertEquals
import org.junit.Test

class BranchHistoryTest : AllChronoDBBackendsTest() {

    @Test
    fun canGetHistoryOfKeyOnBranch() {
        val db = this.chronoDB

        val tx1 = db.tx()
        tx1.put("hello", "world")
        tx1.put("foo", "bar")
        val commit1 = tx1.commit()

        val tx2 = db.tx()
        tx2.put("foo", "baz")
        val commit2 = tx2.commit()

        db.branchManager.createBranch("MyBranch")


        val tx3 = db.tx()
        tx3.remove("hello")
        tx3.put("foo", "bazzzz!")
        val commit3 = tx3.commit()

        val fooHistoryMyBranch = db.tx("MyBranch").history("foo").asSequence().toList()
        assertEquals(listOf(commit2, commit1), fooHistoryMyBranch)

        val fooHistoryMaster = db.tx().history("foo").asSequence().toList()
        assertEquals(listOf(commit3, commit2, commit1), fooHistoryMaster)


        val helloHistoryMyBranch = db.tx("MyBranch").history("hello").asSequence().toList()
        assertEquals(listOf(commit1), helloHistoryMyBranch)

        val helloHistoryMaster = db.tx().history("hello").asSequence().toList()
        assertEquals(listOf(commit3, commit1), helloHistoryMaster)
    }

    @Test
    fun canGetLastModificationTimestampOnBranch(){
        val db = this.chronoDB

        val tx1 = db.tx()
        tx1.put("hello", "world")
        tx1.put("foo", "bar")
        tx1.put("math", "x", 5)
        val commit1 = tx1.commit()

        val tx2 = db.tx()
        tx2.put("foo", "baz")
        tx2.put("math", "x", 42)
        val commit2 = tx2.commit()

        db.branchManager.createBranch("MyBranch")


        val tx3 = db.tx()
        tx3.remove("hello")
        tx3.put("foo", "bazzzz!")
        tx3.put("math", "x", 47)
        val commit3 = tx3.commit()

        println("Commit 1: " + commit1)
        println("Commit 2: " + commit2)
        println("Commit 3: " + commit3)

        println("Branching Timestamp: " + db.branchManager.getBranch("MyBranch").branchingTimestamp)

        val helloLastModifiedMyBranch = db.tx("MyBranch").getLastModificationTimestamp("hello")
        val fooLastModifiedMyBranch = db.tx("MyBranch").getLastModificationTimestamp("foo")
        val mathXLastModifiedMyBranch = db.tx("MyBranch").getLastModificationTimestamp("math", "x")

        val helloLastModifiedMaster = db.tx().getLastModificationTimestamp("hello")
        val fooLastModifiedMaster = db.tx().getLastModificationTimestamp("foo")
        val mathXLastModifiedMaster = db.tx().getLastModificationTimestamp("math", "x")

        assertEquals(commit1, helloLastModifiedMyBranch)
        assertEquals(commit2, fooLastModifiedMyBranch)
        assertEquals(commit2, mathXLastModifiedMyBranch)

        assertEquals(commit3, helloLastModifiedMaster)
        assertEquals(commit3, fooLastModifiedMaster)
        assertEquals(commit3, mathXLastModifiedMaster)
    }

    @Test
    fun canGetLastModificationTimestampOnBranchBeforeBranchingTimestamp(){
        val db = this.chronoDB

        val tx1 = db.tx()
        tx1.put("hello", "world")
        tx1.put("foo", "bar")
        tx1.put("math", "x", 5)
        val commit1 = tx1.commit()

        sleep(5)

        val tx2 = db.tx()
        tx2.put("foo", "baz")
        val commit2 = tx2.commit()

        db.branchManager.createBranch("MyBranch")

        val tx3 = db.tx()
        tx3.remove("foo")
        tx3.commit()

        val tx4 = db.tx("MyBranch")
        tx4.put("foo", "lala")
        tx4.commit()


        val requestTimestamp = (commit2 - commit1) / 2 + commit1

        val readTx = db.tx("MyBranch", requestTimestamp)
        assertEquals(commit1, readTx.getLastModificationTimestamp("foo"))

    }



}