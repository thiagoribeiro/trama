package run.trama.saga

import run.trama.config.RateLimitConfig
import run.trama.saga.redis.RedisBinaryCommands
import run.trama.saga.redis.RedisCommandsProvider
import run.trama.saga.redis.RedisShardKeyspace
import run.trama.saga.redis.RedisSagaRateLimiter
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking

class RateLimiterTest {
    @Test
    fun `rate limiter blocks after max failures`() = runBlocking {
        var count = 0L
        var blocked: ByteArray? = null

        val commands = object : RedisBinaryCommands {
            override suspend fun zadd(key: ByteArray, score: Double, member: ByteArray): Long? = 1L
            override suspend fun zrem(key: ByteArray, member: ByteArray): Long? = 1L
            override suspend fun zremrangebyscore(key: ByteArray, min: Double, max: Double): Long? = 0L
            override suspend fun zrangebyscore(
                key: ByteArray,
                min: Double,
                max: Double,
                limit: Int?,
            ): List<ByteArray> = emptyList()
            override suspend fun <T> eval(
                script: ByteArray,
                outputType: io.lettuce.core.ScriptOutputType,
                keys: Array<ByteArray>,
                vararg values: ByteArray,
            ): T? = null
            override suspend fun scriptLoad(script: ByteArray): String = ""
            override suspend fun <T> evalsha(digest: String, outputType: io.lettuce.core.ScriptOutputType, keys: Array<ByteArray>, vararg values: ByteArray): T? = null
            override suspend fun get(key: ByteArray): ByteArray? = blocked
            override suspend fun incr(key: ByteArray): Long? = ++count
            override suspend fun pexpire(key: ByteArray, milliseconds: Long): Boolean? = true
            override suspend fun psetex(key: ByteArray, milliseconds: Long, value: ByteArray): String? {
                blocked = value
                return "OK"
            }
            override suspend fun setex(key: ByteArray, seconds: Long, value: ByteArray): String? = "OK"
            override suspend fun expire(key: ByteArray, seconds: Long): Boolean? = true
            override suspend fun set(key: ByteArray, value: ByteArray): String? = "OK"
            override suspend fun setNx(key: ByteArray, value: ByteArray, ttlSeconds: Long): Boolean = true
            override suspend fun del(vararg keys: ByteArray): Long? = keys.size.toLong()
            override suspend fun lpush(key: ByteArray, value: ByteArray): Long? = 1L
            override suspend fun lrange(key: ByteArray, start: Long, stop: Long): List<ByteArray> = emptyList()
            override suspend fun ping(): String = "PONG"
        }

        val provider: RedisCommandsProvider = object : RedisCommandsProvider {
            override suspend fun <T> withCommands(
                block: suspend (RedisBinaryCommands) -> T
            ): T = block(commands)
        }

        val limiter = RedisSagaRateLimiter(
            redis = provider,
            config = RateLimitConfig(
                enabled = true,
                maxFailures = 2,
                windowMillis = 60_000,
                blockMillis = 60_000,
            ),
            keyspace = RedisShardKeyspace("saga:executions", 1024),
        )

        limiter.recordFailure("order")
        assertNull(limiter.checkDelayMillis("order"))
        limiter.recordFailure("order")
        assertNotNull(limiter.checkDelayMillis("order"))
    }
}
