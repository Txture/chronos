package org.chronos.chronodb.exodus.test.cases

import jetbrains.exodus.log.TooBigLoggableException
import org.apache.commons.io.FileUtils
import org.chronos.chronodb.api.ChronoDB
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.toByteIterable
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.chronos.common.testing.kotlin.ext.notBeNull
import org.chronos.common.testing.kotlin.ext.should
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.nio.file.Files

@Tag("IntegrationTest")
class SizeLimitTest {

    @Test
    fun errorIsThrownWhenKeyValuePairIsTooLarge() {
        val tempDir = Files.createTempDirectory("chronodb").toFile()
        try {
            ChronoDB.FACTORY.create().database(ExodusChronoDB.BUILDER).onFile(tempDir).build().use { chronoDB ->
                chronoDB should notBeNull()

                val keyLength = UnqualifiedTemporalKey.createMin("hello").toByteIterable().length
                run {
                    val tx = chronoDB.tx()
                    val array1 = ByteArray(8 * 1024 * 1024 - keyLength - 16)
                    // this should be okay
                    tx.put("hello", array1)
                    tx.commit()
                }

                // this should throw an error
                run {
                    val tx = chronoDB.tx()
                    val array2 = ByteArray(8 * 1024 * 1024 - keyLength - 16 + 1)
                    try {
                        tx.put("hello", array2)
                        tx.commit()
                        fail("Should have thrown an exception")
                    } catch (expected: ChronoDBCommitException) {
                        assert(expected.cause is TooBigLoggableException)
                        assert(expected.cause?.message?.contains("Key: ") ?: false)
                    }
                    tx.rollback()
                }
            }
        } finally {
            FileUtils.deleteQuietly(tempDir)
        }
    }

}