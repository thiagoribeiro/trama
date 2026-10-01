@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import io.lettuce.core.ScriptOutputType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromByteArray
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
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

    private val logger = LoggerFactory.getLogger(SagaExecutionRedisConsumer::class.java)

    /**
     * Claims one batch from a shard. It first moves this shard's expired in-flight items (claimed by
     * a pod that died or stalled) back to ready, so recovery happens on the claimers' normal pass
     * over every owned shard — within processingTimeoutMillis plus one poll — at the cost of one
     * extra ZRANGEBYSCORE that is empty in the common case, and no extra round trip. Both keys
     * share the shard's hash slot, so this stays valid on Redis Cluster.
     */
    private val claimScript = """
        local ready = KEYS[1]
        local inflight = KEYS[2]
        local now = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local inflightScore = tonumber(ARGV[3])
        local expired = redis.call('ZRANGEBYSCORE', inflight, '-inf', now, 'LIMIT', 0, limit)
        if #expired > 0 then
            redis.call('ZREM', inflight, unpack(expired))
            for i = 1, #expired do
                redis.call('ZADD', ready, now, expired[i])
            end
        end
        local items = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, limit)
        if #items > 0 then
            redis.call('ZREM', ready, unpack(items))
            for i = 1, #items do
                redis.call('ZADD', inflight, inflightScore, items[i])
            end
        end
        return items
    """.trimIndent()

    /** Pushes the in-flight deadline of claims still being processed; skips any no longer in flight. */
    private val renewClaimsScript = """
        local inflight = KEYS[1]
        local score = ARGV[1]
        local renewed = 0
        for i = 2, #ARGV do
            if redis.call('ZSCORE', inflight, ARGV[i]) then
                redis.call('ZADD', inflight, score, ARGV[i])
                renewed = renewed + 1
            end
        end
        return renewed
    """.trimIndent()

    /** A claim handed to the processor and not yet acked or released. */
    private class LiveClaim(val shardId: Int, val payload: ByteArray, @Volatile var renewedAtMillis: Long)

    private val liveClaims = ConcurrentHashMap<Long, LiveClaim>()
    private val claimIds = AtomicLong()

    // SHA1 digests loaded via SCRIPT LOAD at startup — avoids sending the full script on every poll
    private var claimScriptSha: String? = null
    private var renewClaimsScriptSha: String? = null

    /** Must be called once before the consumer loop starts. Loads scripts into Redis script cache. */
    suspend fun loadScripts() {
        redis.withCommands { commands ->
            claimScriptSha = commands.scriptLoad(claimScript.toByteArray())
            renewClaimsScriptSha = commands.scriptLoad(renewClaimsScript.toByteArray())
        }
    }

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
        liveClaims.remove(inFlight.claimId)
        redis.withCommands { commands ->
            commands.zrem(keyspace.queueInFlightKey(inFlight.shardId).encodeToByteArray(), inFlight.payload)
        }
    }

    override fun release(inFlight: ClaimedExecution) {
        liveClaims.remove(inFlight.claimId)
    }

    /**
     * Keeps every live claim's in-flight deadline ahead of processingTimeoutMillis while it is being
     * worked on (or waits in the processor buffer), so a slow but healthy execution is never
     * re-delivered to another worker. When this pod dies the renewals stop, and the claims expire
     * and are recovered by the shard owner's next claim pass. Only claims older than a third of
     * the timeout are renewed, so the usual sub-second executions cost nothing here.
     * Must run until the processor has drained (see RuntimeBootstrap.stop).
     */
    suspend fun runClaimHeartbeat() {
        val interval = (processingTimeoutMillis / 3).coerceAtLeast(1)
        while (currentCoroutineContext().isActive) {
            delay(interval.milliseconds)
            runCatching { renewDueClaims(interval) }
                .onFailure { logger.warn("claim heartbeat failed", it) }
        }
    }

    private suspend fun renewDueClaims(interval: Long) {
        val now = System.currentTimeMillis()
        val due = liveClaims.values.filter { now - it.renewedAtMillis >= interval }
        if (due.isEmpty()) return
        val score = (now + processingTimeoutMillis).toString().toByteArray()
        for ((shardId, claims) in due.groupBy { it.shardId }) {
            for (batch in claims.chunked(RENEW_BATCH_SIZE)) {
                val args = arrayOf(score) + batch.map { it.payload }
                val keys = arrayOf(keyspace.queueInFlightKey(shardId).encodeToByteArray())
                val renewed = redis.withCommands { commands ->
                    val sha = renewClaimsScriptSha
                    if (sha != null) {
                        commands.evalsha<Long>(sha, ScriptOutputType.INTEGER, keys, *args)
                    } else {
                        commands.eval<Long>(renewClaimsScript.toByteArray(), ScriptOutputType.INTEGER, keys, *args)
                    }
                } ?: 0L
                batch.forEach { it.renewedAtMillis = now }
                if (renewed < batch.size) {
                    // Deadline passed before this renewal (e.g. a long GC pause or Redis outage):
                    // those items were already recovered and may run again (at-least-once).
                    logger.warn("claims expired before renewal", kv("shardId", shardId), kv("lost", batch.size - renewed))
                }
            }
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
            val sha = claimScriptSha
            if (sha != null) {
                commands.evalsha<List<ByteArray>>(
                    sha,
                    ScriptOutputType.MULTI,
                    arrayOf(readyKey, inFlightKey),
                    now.toString().toByteArray(),
                    limit.toString().toByteArray(),
                    inflightScore.toString().toByteArray(),
                )
            } else {
                commands.eval<List<ByteArray>>(
                    claimScript.toByteArray(),
                    ScriptOutputType.MULTI,
                    arrayOf(readyKey, inFlightKey),
                    now.toString().toByteArray(),
                    limit.toString().toByteArray(),
                    inflightScore.toString().toByteArray(),
                )
            }
        } ?: emptyList()

        if (items.isEmpty()) return emptyList()

        return items.map { payload ->
            val execution = msgPack.decodeFromByteArray(SagaExecution.serializer(), payload)
            metrics.recordDequeued(execution)
            val claimId = claimIds.incrementAndGet()
            liveClaims[claimId] = LiveClaim(shardId, payload, now)
            ClaimedExecution(execution, payload, shardId, claimId)
        }
    }

    private fun assignedShards(
        ownedShards: List<Int>,
        claimerIndex: Int,
    ): List<Int> = ownedShards.filterIndexed { index, _ -> index % effectiveClaimerCount == claimerIndex }

    private companion object {
        /** Members per renew script call, bounding the size of a single EVAL. */
        const val RENEW_BATCH_SIZE = 100
    }
}
