package org.chronos.chronodb.test.cases

import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.internal.api.BranchInternal
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest
import org.chronos.common.test.junit.categories.IntegrationTest
import org.hamcrest.Matchers.`is`
import org.junit.Assert.assertThat
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(IntegrationTest::class)
class DumpLoadKeyspaceMetadataTest : AllChronoDBBackendsTest(){

    val dumpFileName = "changesAtCommitTimestampDump2.xml"
    val lowestTimestampInXML = 1536911128414L

    @Test
    fun navigationIndexIsUpdatedWhenDumpIsLoaded(){
        val dumpXmlFile = this.getSrcTestResourcesFile(dumpFileName)
        val db = this.chronoDB
        try{
            db.backupManager.readDump(dumpXmlFile)

            val masterBranch = db.branchManager.getBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER) as BranchInternal
            val tkvs = masterBranch.temporalKeyValueStore as AbstractTemporalKeyValueStore
            val matrix = tkvs.getMatrix("mykeyspace")
            assertThat(matrix.creationTimestamp, `is`(lowestTimestampInXML))
        }finally{
            db.close()
        }

    }

}