package org.chronos.chronodb.test.cases

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.junit.Assert.*
import org.junit.Test
import org.junit.experimental.categories.Category
import kotlin.math.exp

@Category(IntegrationTest::class)
class DeleteBranchesTest : AllChronoDBBackendsTest() {

    @Test
    fun canDeleteSingleBranch(){
        val db = this.chronoDB
        run {
            val tx = db.tx()
            tx.put("hello", "world")
            tx.commit()
        }
        val branch = db.branchManager.createBranch("sub")
        run {
            val tx = db.tx(branch.name)
            tx.put("foo", "bar")
            tx.commit()
        }
        db.branchManager.deleteBranchRecursively(branch.name)
        assertFalse(db.branchManager.existsBranch("sub"))
    }

    @Test
    fun canDeleteBranchRecursively(){
        val db = this.chronoDB
        run {
            val tx = db.tx()
            tx.put("hello", "world")
            tx.commit()
        }
        val branch = db.branchManager.createBranch("sub")
        run {
            val tx = db.tx(branch.name)
            tx.put("foo", "bar")
            tx.commit()
        }
        db.branchManager.createBranch("sub", "sub2")
        db.branchManager.deleteBranchRecursively(branch.name)

        assertFalse(db.branchManager.existsBranch("sub"))
        assertFalse(db.branchManager.existsBranch("sub2"))
    }

    @Test
    fun canDeleteChildBranch(){
        val db = this.chronoDB
        run {
            val tx = db.tx()
            tx.put("hello", "world")
            tx.commit()
        }
        val branch = db.branchManager.createBranch("sub")
        run {
            val tx = db.tx(branch.name)
            tx.put("foo", "bar")
            tx.commit()
        }
        db.branchManager.createBranch("sub", "sub2")
        db.branchManager.deleteBranchRecursively("sub2")

        assertTrue(db.branchManager.existsBranch("sub"))
        assertFalse(db.branchManager.existsBranch("sub2"))
    }

    @Test
    fun cannotDeleteMaster(){
        val db = this.chronoDB
        run {
            val tx = db.tx()
            tx.put("hello", "world")
            tx.commit()
        }
        try{
            db.branchManager.deleteBranchRecursively(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)
            fail("Managed to delete master branch")
        }catch(expected: IllegalArgumentException){
            // pass
        }
    }

}