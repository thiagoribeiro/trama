package run.trama.saga

import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.mockk
import run.trama.config.RateLimitConfig
import run.trama.saga.redis.RedisCommandsProvider
import run.trama.saga.redis.RedisSagaRateLimiter
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking

class RateLimiterTest {
    @Test
    fun `rate limiter blocks after max failures`() = runBlocking {
        val commands = mockk<RedisCoroutinesCommands<ByteArray, ByteArray>>()
        var count = 0L
        val blocked = mutableListOf<ByteArray?>()

        coEvery { commands.get(any()) } answers { blocked.firstOrNull() }
        coEvery { commands.incr(any()) } answers { ++count }
        coEvery { commands.pexpire(any<ByteArray>(), any<Long>()) } returns true
        coEvery { commands.psetex(any(), any(), any<ByteArray>()) } answers {
            val value = thirdArg<ByteArray>()
            blocked.clear()
            blocked.add(value)
            "OK"
        }

        val provider: RedisCommandsProvider = object : RedisCommandsProvider {
            override suspend fun <T> withCommands(
                block: suspend (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T
            ): T = block(commands)
        }

        val limiter = RedisSagaRateLimiter(
            redis = provider,
            config = RateLimitConfig(
                enabled = true,
                maxFailures = 2,
                windowMillis = 60_000,
                blockMillis = 60_000,
            )
        )

        limiter.recordFailure("order")
        assertNull(limiter.checkDelayMillis("order"))
        limiter.recordFailure("order")
        assertNotNull(limiter.checkDelayMillis("order"))
    }
}
