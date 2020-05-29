package org.chronos.chronodb.exodus.provider

import org.apache.commons.configuration.Configuration
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider
import org.chronos.chronodb.api.builder.database.spi.TestSuitePlugin
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.builder.ExodusChronoDBBuilderImpl
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration
import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.chronodb.internal.api.ChronoDBInternal
import org.chronos.common.configuration.ChronosConfigurationUtil

class ExodusChronoDBBackendProvider : ChronoDBBackendProvider {

    private val name = "xodus"
    private val names = setOf("exodus", name)

    private val testSuitePlugin: TestSuitePlugin

    constructor(){
        this.testSuitePlugin = ExodusChronoDBTestSuitePlugin()
    }

    override fun matchesBackendName(backendName: String): Boolean {
        return names.contains(backendName)
    }

    override fun getConfigurationClass(): Class<out ChronoDBConfiguration> {
       return ExodusChronoDBConfiguration::class.java
    }

    override fun instantiateChronoDB(configuration: Configuration): ChronoDBInternal {
        require(matchesBackendName(configuration.getProperty(ChronoDBConfiguration.STORAGE_BACKEND) as String)) {
            "Illegal argument: the given configuration specifies the backend '${configuration.getProperty(ChronoDBConfiguration.STORAGE_BACKEND)}', which is not supported by this builder! Supported backend names of this builder are: ${this.names}"
        }
        val config = ChronosConfigurationUtil.build(configuration, ExodusChronoDBConfiguration::class.java)
        return ExodusChronoDB(config)
    }

    override fun getBackendName(): String {
        return this.name
    }

    override fun getTestSuitePlugin(): TestSuitePlugin {
        return this.testSuitePlugin
    }

    override fun createBuilder(): ChronoDBBackendBuilder {
        return ExodusChronoDBBuilderImpl()
    }

}