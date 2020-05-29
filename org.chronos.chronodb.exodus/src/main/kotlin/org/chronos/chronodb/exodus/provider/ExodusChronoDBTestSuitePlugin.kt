package org.chronos.chronodb.exodus.provider

import org.apache.commons.configuration.BaseConfiguration
import org.apache.commons.configuration.Configuration
import org.chronos.chronodb.api.builder.database.spi.TestSuitePlugin
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration
import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.common.exceptions.ChronosIOException
import java.io.File
import java.io.IOException
import java.lang.reflect.Method
import java.nio.file.Files
import java.util.*

class ExodusChronoDBTestSuitePlugin: TestSuitePlugin {

    override fun createBasicTestConfiguration(testMethod: Method, testDirectory: File): Configuration {
        val configuration = BaseConfiguration()
        configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, ExodusChronoDB.BACKEND_NAME)
        configuration.setProperty(ExodusChronoDBConfiguration.WORK_DIR, this.createDBDirectory(testDirectory))
        return configuration
    }

    override fun onBeforeTest(testClass: Class<*>?, testMethod: Method?, testDirectory: File?) {
    }

    override fun onAfterTest(testClass: Class<*>?, testMethod: Method?, testDirectory: File?) {
    }

    private fun createDBDirectory(parentDir: File): File{
        val dbDir = File(parentDir, UUID.randomUUID().toString().replace("-".toRegex(), "") + ".chronodb")
        try {
            val path = dbDir.toPath()
            Files.deleteIfExists(path)
            Files.createDirectory(path)
        } catch (e: IOException) {
            throw ChronosIOException("Failed to create DB directory in test directory '" + parentDir.absolutePath + "'!")
        }
        return dbDir
    }
}