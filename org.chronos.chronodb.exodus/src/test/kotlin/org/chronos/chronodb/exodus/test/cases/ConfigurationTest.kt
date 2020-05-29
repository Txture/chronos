package org.chronos.chronodb.exodus.test.cases

import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration
import org.chronos.common.testing.kotlin.ext.notBeNull
import org.chronos.common.testing.kotlin.ext.should
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.hamcrest.CoreMatchers.instanceOf
import org.junit.Test
import java.nio.file.Files

class ConfigurationTest {

    @Test
    fun canCreateExodusDatabaseWithBuilder(){
        val tempDir = Files.createTempDirectory("chronodb").toFile()
        try{
            val chronoDB = ChronoDB.FACTORY.create().database(ExodusChronoDB.BUILDER).onFile(tempDir).build()
            chronoDB should notBeNull()
            chronoDB.features.isPersistent shouldBe true
            chronoDB.features.isRolloverSupported shouldBe true
            chronoDB.configuration shouldBe instanceOf(ExodusChronoDBConfiguration::class.java)
            val config = chronoDB.configuration as ExodusChronoDBConfiguration
            config.workDirectory shouldBe tempDir
            val tx = chronoDB.tx()
            tx.put("hello", "world")
            tx.commit()
        }finally {
            FileUtils.deleteQuietly(tempDir)
        }

    }

}