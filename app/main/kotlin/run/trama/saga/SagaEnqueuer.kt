@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import run.trama.saga.redis.DueIndex
import run.trama.saga.redis.RedisCommandsProvider
import run.trama.saga.redis.RedisShardKeyspace
import run.trama.telemetry.Metrics
import kotlinx.serialization.encodeToByteArray

interface SagaEnqueuer {
    suspend fun enqueue(execution: SagaExecution, delayMillis: Long)
}

class RedisSagaEnqueuer(
    private val redis: RedisCommandsProvider,
    private val keyspace: RedisShardKeyspace,
    /** Feeds saga_enqueue_total; optional so tests can build an enqueuer without a registry. */
    private val metrics: Metrics? = null,
) : SagaEnqueuer {
    private val msgPack = MsgPack()
    private val dueKey = keyspace.queueDueKey().encodeToByteArray()

    override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
        val score = System.currentTimeMillis() + delayMillis
        val payload = msgPack.encodeToByteArray(SagaExecution.serializer(), execution)
        val shardId = keyspace.virtualShardFor(execution.id)
        val redisKey = keyspace.queueReadyKey(shardId).encodeToByteArray()
        redis.withCommands { commands ->
            commands.zadd(redisKey, score.toDouble(), payload)
            // After the ZADD: a claimer that drops this mark first still finds the item when it
            // claims. A crash in between leaves the item for the claimers' periodic full sweep.
            DueIndex.mark(commands, dueKey, listOf(shardId.toString(), score.toString()))
        }
        metrics?.recordEnqueued(execution)
    }
}
