@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import io.lettuce.core.ScriptOutputType
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromByteArray
import run.trama.saga.SagaExecution
import run.trama.telemetry.Metrics
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds

class SagaExecutionRedisConsumer(
    private val redis: RedisCommandsProvider,
    private val keyspace: RedisShardKeyspace,
    private val allocator: RendezvousShardAllocator,
    private val batchSize: Int,
    private val processingTimeoutMillis: Long,
    private val claimerCount: Int,
    private val metrics: Metrics,
) : SagaExecutionConsumer {
    private val msgPack = MsgPack()
    private val polling = AtomicBoolean(true)
    private val effectiveClaimerCount = claimerCount.coerceAtLeast(1)
    private val claimLimit = max(1, batchSize / effectiveClaimerCount)

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

    override suspend fun runProducer(
        buffer: kotlinx.coroutines.channels.SendChannel<ClaimedExecution>,
        emptyPollDelayMillis: Long,
    ) = coroutineScope {
        repeat(effectiveClaimerCount) { claimerIndex ->
            launch {
                var cursor = 0
                while (polling.get() && currentCoroutineContext().isActive) {
                    val shards = assignedShards(allocator.ownedShards(), claimerIndex)
                    if (shards.isEmpty()) {
                        if (!polling.get()) break
                        delay(emptyPollDelayMillis.milliseconds)
                        continue
                    }

                    var claimedAny = false
                    for (offset in shards.indices) {
                        if (!polling.get()) break
                        val shardId = shards[(cursor + offset) % shards.size]
                        val items = pollReady(shardId, claimLimit)
                        if (items.isNotEmpty()) {
                            claimedAny = true
                            items.forEach { buffer.send(it) }
                        }
                    }
                    cursor = (cursor + 1) % shards.size

                    if (!claimedAny && polling.get()) {
                        delay(emptyPollDelayMillis.milliseconds)
                    }
                }
            }
        }
    }

    override fun stopPolling() {
        polling.set(false)
    }

    override suspend fun ack(inFlight: ClaimedExecution) {
        redis.withCommands { commands ->
            commands.zrem(keyspace.queueInFlightKey(inFlight.shardId).encodeToByteArray(), inFlight.payload)
        }
    }

    suspend fun runExpiredRequeuePoller(
        intervalMillis: Long,
        limit: Int = batchSize,
    ) {
        var cursor = 0
        while (polling.get() && currentCoroutineContext().isActive) {
            val shards = allocator.ownedShards()
            if (shards.isNotEmpty()) {
                val shardId = shards[cursor % shards.size]
                requeueExpired(shardId, limit)
                cursor = (cursor + 1) % shards.size
            }
            if (!polling.get()) break
            delay(intervalMillis.milliseconds)
        }
    }

    private suspend fun pollReady(
        shardId: Int,
        limit: Int,
    ): List<ClaimedExecution> {
        val now = System.currentTimeMillis()
        val inflightScore = now + processingTimeoutMillis
        val readyKey = keyspace.queueReadyKey(shardId).encodeToByteArray()
        val inFlightKey = keyspace.queueInFlightKey(shardId).encodeToByteArray()

        val items = redis.withCommands { commands ->
            metrics.recordRedisClaimScan()
            commands.eval<List<ByteArray>>(
                claimScript.toByteArray(),
                ScriptOutputType.MULTI,
                arrayOf(readyKey, inFlightKey),
                now.toString().toByteArray(),
                limit.toString().toByteArray(),
                inflightScore.toString().toByteArray(),
            )
        } ?: emptyList()

        if (items.isEmpty()) return emptyList()

        return items.map { payload ->
            val execution = msgPack.decodeFromByteArray(SagaExecution.serializer(), payload)
            metrics.recordDequeued(execution)
            ClaimedExecution(execution, payload, shardId)
        }
    }

    private suspend fun requeueExpired(
        shardId: Int,
        limit: Int,
    ) {
        val now = System.currentTimeMillis()
        val inFlightKey = keyspace.queueInFlightKey(shardId).encodeToByteArray()
        val readyKey = keyspace.queueReadyKey(shardId).encodeToByteArray()
        redis.withCommands { commands ->
            commands.eval<List<ByteArray>>(
                requeueExpiredScript.toByteArray(),
                ScriptOutputType.MULTI,
                arrayOf(inFlightKey, readyKey),
                now.toString().toByteArray(),
                limit.toString().toByteArray(),
            )
        }
    }

    private fun assignedShards(
        ownedShards: List<Int>,
        claimerIndex: Int,
    ): List<Int> = ownedShards.filterIndexed { index, _ -> index % effectiveClaimerCount == claimerIndex }
}
