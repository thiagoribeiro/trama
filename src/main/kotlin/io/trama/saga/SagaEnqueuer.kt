@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import run.trama.saga.redis.RedisCommandsProvider
import kotlinx.serialization.encodeToByteArray

interface SagaEnqueuer {
    suspend fun enqueue(execution: SagaExecution, delayMillis: Long)
}

class RedisSagaEnqueuer(
    private val redis: RedisCommandsProvider,
    private val redisKey: String = "saga:executions",
) : SagaEnqueuer {
    private val msgPack = MsgPack()

    override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
        val score = System.currentTimeMillis() + delayMillis
        val payload = msgPack.encodeToByteArray(SagaExecution.serializer(), execution)
        redis.withCommands { commands ->
            commands.zadd(redisKey.encodeToByteArray(), score.toDouble(), payload)
        }
    }
}
