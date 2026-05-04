package run.trama.saga.redis

import java.nio.charset.StandardCharsets
import run.trama.config.RateLimitConfig

interface SagaRateLimiter {
    suspend fun checkDelayMillis(sagaName: String): Long?
    suspend fun recordFailure(sagaName: String)
}

@OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)
class RedisSagaRateLimiter(
    private val redis: RedisCommandsProvider,
    private val config: RateLimitConfig,
    private val keyspace: RedisShardKeyspace,
) : SagaRateLimiter {
    override suspend fun checkDelayMillis(sagaName: String): Long? {
        if (!config.enabled) return null
        val blockedKey = keyspace.rateLimitBlockedKey(sagaName, config.keyPrefix)
        return redis.withCommands { commands ->
            val value = commands.get(blockedKey)
            if (value == null) return@withCommands null
            val unblockAt = value.toStringUtf8().toLongOrNull() ?: return@withCommands null
            val now = System.currentTimeMillis()
            if (unblockAt <= now) null else (unblockAt - now)
        }
    }

    override suspend fun recordFailure(sagaName: String) {
        if (!config.enabled) return
        val counterKey = keyspace.rateLimitCountKey(sagaName, config.keyPrefix)
        val blockedKey = keyspace.rateLimitBlockedKey(sagaName, config.keyPrefix)
        redis.withCommands { commands ->
            val count = commands.incr(counterKey) ?: 0L
            if (count == 1L) {
                commands.pexpire(counterKey, config.windowMillis)
            }
            if (count >= config.maxFailures) {
                val unblockAt = System.currentTimeMillis() + config.blockMillis
                commands.psetex(blockedKey, config.blockMillis, unblockAt.toString().toByteArray())
            }
        }
    }
}

private fun ByteArray.toStringUtf8(): String =
    String(this, StandardCharsets.UTF_8)
