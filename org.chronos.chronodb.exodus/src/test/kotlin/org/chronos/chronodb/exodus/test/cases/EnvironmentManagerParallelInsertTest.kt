package org.chronos.chronodb.exodus.test.cases

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.StoreConfig
import mu.KotlinLogging
import org.chronos.chronodb.exodus.test.base.EnvironmentTest
import org.chronos.common.test.utils.TestUtils
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.File
import java.lang.Thread.sleep
import java.util.stream.IntStream
import kotlin.system.measureTimeMillis

@Tag("slow")
class EnvironmentManagerParallelInsertTest : EnvironmentTest(20, 5) {

    companion object {

        private val log = KotlinLogging.logger {}

    }

    @Test
    fun environmentManagerParallelInsertTest() {
        val numberOfFolders = 100
        val folders = (0 until numberOfFolders).map { File(this.testDir, "env${it}") }.toList()

        log.info("Sleeping")
        sleep(1000)
        log.info("Ready")

        measureTimeMillis {
            val thread1 = Thread({ doWork(folders, 0, 250_000) }, "Thread 1")
            val thread2 = Thread({ doWork(folders, 250_000, 500_000) }, "Thread 2")
            val thread3 = Thread({ doWork(folders, 500_000, 750_000) }, "Thread 3")
            val thread4 = Thread({ doWork(folders, 750_000, 1_000_000) }, "Thread 4")
            thread1.start()
            thread2.start()
            thread3.start()
            thread4.start()
            thread1.join()
            thread2.join()
            thread3.join()
            thread4.join()
        }.let {
            println("Execution of environmentManagerParallelInsertTest took ${it}ms. (${System.currentTimeMillis()})")
        }
    }


    private fun doWork(folders: List<File>, lowerBound: Int, upperBound: Int) {
        IntStream.range(lowerBound, upperBound).forEach { i ->
            val randomDir = TestUtils.getRandomEntryOf(folders)
            val env = this.getEnvironment(randomDir)
            env.executeInTransaction { tx ->
                val store = env.openStore("test", StoreConfig.WITHOUT_DUPLICATES, tx)
                store.put(tx, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i))
                tx.commit()
            }
        }
        println("Thread ${Thread.currentThread().name} is done.")

    }
}