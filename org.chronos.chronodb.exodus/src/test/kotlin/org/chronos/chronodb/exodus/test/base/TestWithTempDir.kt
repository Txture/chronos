package org.chronos.chronodb.exodus.test.base

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.lang.IllegalStateException

abstract class TestWithTempDir {

    private var dir: File? = null

    val testDir: File
        get() = this.dir ?: throw IllegalStateException("Test dir is not set up yet!")


    @BeforeEach
    fun setUpTestDir() {
        dir = Files.createTempDir()
        dir!!.deleteOnExit()
    }

    @AfterEach
    fun tearDownTestDir() {
        if (this.dir != null) {
            FileUtils.deleteDirectory(this.dir)
        }
    }

}