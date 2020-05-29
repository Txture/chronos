package org.chronos.chronodb.exodus.test.base

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentConfig
import org.chronos.chronodb.exodus.environment.EnvironmentManager
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.File


abstract class EnvironmentTest : TestWithTempDir {

    private val environmentManager: EnvironmentManager
    private var env: Environment? = null

    val environment: Environment
        get() = this.env!!


    @BeforeEach
    fun beforeEach() {
        this.env = this.environmentManager.getEnvironment(this.testDir)
    }

    @AfterEach
    fun afterEach() {
        this.environmentManager.cleanupEnvironments(true)
    }

    constructor(environmentsToKeep: Int = 10, cleanupTaskDelaySeconds: Int = 60){
        this.environmentManager = EnvironmentManager(emptyMap(), environmentsToKeep, cleanupTaskDelaySeconds)
    }

    fun getEnvironment(file: File): Environment{
        return this.environmentManager.getEnvironment(file)
    }

}