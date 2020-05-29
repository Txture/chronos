package org.chronos.chronodb.exodus.test.cases

import org.chronos.chronodb.algorithms.temporalGet
import org.chronos.common.testing.kotlin.ext.shouldBe
import org.chronos.common.testing.kotlin.ext.shouldBeNull
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey
import org.junit.jupiter.api.Test

class TemporalGetTest {

    @Test
    fun getWorksOnEmptyMatrix() {
        val result = temporalGet("default", "my-key", 100, null, null)
        result.isHit shouldBe false
        result.period shouldBe Period.eternal()
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }

    @Test
    fun getWorksOnExactHead() {
        val result = temporalGet("default", "my-key", 100, Pair(UnqualifiedTemporalKey("my-key", 100), "myValue".toByteArray()), null)
        result.isHit shouldBe true
        result.period shouldBe Period.createOpenEndedRange(100)
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }

    @Test
    fun getWorksOnHead() {
        val result = temporalGet("default", "my-key", 150, Pair(UnqualifiedTemporalKey("my-key", 100), "myValue".toByteArray()), null)
        result.isHit shouldBe true
        result.period shouldBe Period.createOpenEndedRange(100)
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }

    @Test
    fun getWorksIfKeyIsAbsentFromMatrix() {

        val result = temporalGet("default", "my-key", 100,
                Pair(
                        UnqualifiedTemporalKey("some-other-key", 100),
                        ByteArray(10)
                ),
                Pair(
                        UnqualifiedTemporalKey("yet-another-key", 50),
                        ByteArray(10)
                )
        )
        result.isHit shouldBe false
        result.period shouldBe Period.eternal()
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }

    @Test
    fun getWorksIfKeyDoesNotExistYet() {
        val result = temporalGet("default", "my-key", 100,
                Pair(
                        UnqualifiedTemporalKey("some-other-key", 100),
                        ByteArray(10)
                ),
                Pair(
                        UnqualifiedTemporalKey("my-key", 200),
                        ByteArray(10)
                )
        )
        result.isHit shouldBe false
        result.period shouldBe Period.createRange(0, 200)
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }


    @Test
    fun getWorksIfKeyWasDeleted() {
        val result = temporalGet("default", "my-key", 100,
                Pair(
                        UnqualifiedTemporalKey("my-key", 90),
                        ByteArray(0)
                ),
                Pair(
                        UnqualifiedTemporalKey("my-key", 200),
                        ByteArray(10)
                )
        )
        result.isHit shouldBe true
        result.value.shouldBeNull()
        result.period shouldBe Period.createRange(90, 200)
        result.requestedKey.keyspace shouldBe "default"
        result.requestedKey.key shouldBe "my-key"
    }

}