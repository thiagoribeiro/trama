@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import run.trama.saga.SagaExecution
import run.trama.telemetry.Metrics
import kotlinx.serialization.decodeFromByteArray
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class SagaExecutionRedisConsumer(
    private val redis: RedisCommandsProvider,
    private val readyKey: String,
    private val inFlightKey: String,
    private val batchSize: Int,
    private val processingTimeoutMillis: Long,
    private val metrics: Metrics,
) {
    private val msgPack = MsgPack()

    private val readyKeyBytes: ByteArray = readyKey.encodeToByteArray()
    private val inFlightKeyBytes: ByteArray = inFlightKey.encodeToByteArray()

    private val claimScript = """
        local ready = KEYS[1]
        local inflight = KEYS[2]
        local now = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local inflightScore = tonumber(ARGV[3])
        local items = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, limit)
        if #items > 0 then
            redis.call('ZREM', ready, unpack(items))
            for i = 1, #items do
                redis.call('ZADD', inflight, inflightScore, items[i])
            end
        end
        return items
    """.trimIndent()

    private val requeueExpiredScript = """
        local inflight = KEYS[1]
        local ready = KEYS[2]
        local now = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local items = redis.call('ZRANGEBYSCORE', inflight, '-inf', now, 'LIMIT', 0, limit)
        if #items > 0 then
            redis.call('ZREM', inflight, unpack(items))
            for i = 1, #items do
                redis.call('ZADD', ready, now, items[i])
            end
        end
        return items
    """.trimIndent()

    suspend fun pollReady(): List<InFlightExecution> {
        val now = System.currentTimeMillis()
        val inflightScore = now + processingTimeoutMillis

        val items = redis.withCommands { commands ->
            commands.eval<List<ByteArray>>(
                claimScript.toByteArray(),
                io.lettuce.core.ScriptOutputType.MULTI,
                arrayOf(readyKeyBytes, inFlightKeyBytes),
                now.toString().toByteArray(),
                batchSize.toString().toByteArray(),
                inflightScore.toString().toByteArray(),
            )
        } ?: emptyList()

        if (items.isEmpty()) {
            return emptyList()
        }
        val decoded = items.map { payload ->
            val execution = msgPack.decodeFromByteArray(SagaExecution.serializer(), payload)
            InFlightExecution(execution, payload)
        }
        decoded.forEach { metrics.recordDequeued(it.execution) }
        return decoded
    }

    suspend fun ack(inFlight: InFlightExecution) {
        redis.withCommands { commands ->
            commands.zrem(inFlightKeyBytes, inFlight.payload)
        }
    }

    suspend fun runExpiredRequeuePoller(
        intervalMillis: Long,
        limit: Int = batchSize,
    ) {
        while (true) {
            val now = System.currentTimeMillis()
            redis.withCommands { commands ->
                commands.eval<List<ByteArray>>(
                    requeueExpiredScript.toByteArray(),
                    io.lettuce.core.ScriptOutputType.MULTI,
                    arrayOf(inFlightKeyBytes, readyKeyBytes),
                    now.toString().toByteArray(),
                    limit.toString().toByteArray(),
                )
            }
            delay(intervalMillis.milliseconds)
        }
    }

    data class InFlightExecution(
        val execution: SagaExecution,
        val payload: ByteArray,
    )
}
