@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import io.lettuce.core.ScriptOutputType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
import run.trama.saga.ClaimLease
import run.trama.saga.SagaExecution
import run.trama.telemetry.Metrics
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds

class SagaExecutionRedisConsumer(
    private val redis: RedisCommandsProvider,
    private val keyspace: RedisShardKeyspace,
    private val allocator: RendezvousShardAllocator,
    private val batchSize: Int,
    private val processingTimeoutMillis: Long,
    private val claimerCount: Int,
    private val metrics: Metrics,
    /**
     * How often each claimer visits every owned shard regardless of the due index. A safety net
     * only: it covers index marks lost to a crash between two writes or to a Redis data loss.
     */
    private val fullSweepIntervalMillis: Long = 5_000,
) : SagaExecutionConsumer {
    private val msgPack = MsgPack()
    private val polling = AtomicBoolean(true)
    private val effectiveClaimerCount = claimerCount.coerceAtLeast(1)
    private val claimLimit = max(1, batchSize / effectiveClaimerCount)
    private val leaseMarginMillis = min(2_000L, processingTimeoutMillis / 4)
    private val dueKey = keyspace.queueDueKey().encodeToByteArray()
    private val lastProgressAtMillis = AtomicLong(System.currentTimeMillis())

    private val logger = LoggerFactory.getLogger(SagaExecutionRedisConsumer::class.java)

    /**
     * Claims one batch from a shard. It first moves this shard's expired in-flight items (claimed by
     * a pod that died or stalled) back to ready, so recovery happens on the normal claim pass. Both
     * keys share the shard's hash slot, so this stays valid on Redis Cluster.
     *
     * Returns the earliest time this shard next needs a visit (the first ready item or the first
     * in-flight deadline, "" if neither exists) followed by the claimed members.
     */
    private val claimScript = LuaScript("""
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
        local items = {}
        if limit > 0 then
            items = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, limit)
            if #items > 0 then
                redis.call('ZREM', ready, unpack(items))
                for i = 1, #items do
                    redis.call('ZADD', inflight, inflightScore, items[i])
                end
            end
        end
        local nextAt = nil
        local r = redis.call('ZRANGE', ready, 0, 0, 'WITHSCORES')
        if #r > 0 then nextAt = tonumber(r[2]) end
        local f = redis.call('ZRANGE', inflight, 0, 0, 'WITHSCORES')
        if #f > 0 and (nextAt == nil or tonumber(f[2]) < nextAt) then nextAt = tonumber(f[2]) end
        local result = { nextAt and string.format('%d', nextAt) or '' }
        for i = 1, #items do result[i + 1] = items[i] end
        return result
    """.trimIndent())

    /**
     * Pushes the in-flight deadline of claims still held by this worker. ARGV[1] = new deadline, then
     * (member, deadline this worker last set) pairs. A member whose score differs was recovered and
     * claimed again (possibly by another worker) after the deadline passed, so it is not ours to
     * renew. A missing member is restored: callers only renew claims whose deadline has not passed,
     * which no one else can have taken over, so it can only be missing because Redis lost data.
     * Returns the 1-based pair positions that were lost.
     */
    private val renewClaimsScript = LuaScript("""
        local inflight = KEYS[1]
        local score = ARGV[1]
        local lost = {}
        local pair = 0
        for i = 2, #ARGV, 2 do
            pair = pair + 1
            local current = redis.call('ZSCORE', inflight, ARGV[i])
            if (not current) or tonumber(current) == tonumber(ARGV[i + 1]) then
                redis.call('ZADD', inflight, score, ARGV[i])
            else
                lost[#lost + 1] = pair
            end
        end
        return lost
    """.trimIndent())

    /** Removes a claim only while it is still this worker's (same deadline): never another's. */
    private val ackScript = LuaScript("""
        local current = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if current and tonumber(current) == tonumber(ARGV[2]) then
            return redis.call('ZREM', KEYS[1], ARGV[1])
        end
        return 0
    """.trimIndent())

    /** A claim handed to the processor and not yet acked or released. */
    private class LiveClaim(val shardId: Int, val payload: ByteArray, val lease: ClaimLease, @Volatile var renewedAtMillis: Long)

    private val liveClaims = ConcurrentHashMap<Long, LiveClaim>()
    private val claimIds = AtomicLong()

    /** Time of the last poll that reached Redis, or that found the workers saturated. */
    fun lastProgressAtMillis(): Long = lastProgressAtMillis.get()

    override suspend fun runProducer(
        buffer: kotlinx.coroutines.channels.SendChannel<ClaimedExecution>,
        emptyPollDelayMillis: Long,
        permits: ClaimPermits,
    ) = coroutineScope {
        repeat(effectiveClaimerCount) { claimerIndex ->
            launch { runClaimer(claimerIndex, buffer, emptyPollDelayMillis, permits) }
        }
    }

    /**
     * One claimer's loop. Any Redis failure (outage, restart, timeout) is logged and retried with
     * backoff: the loop must never end while polling is on, or this process silently stops
     * consuming while still accepting work.
     */
    private suspend fun runClaimer(
        claimerIndex: Int,
        buffer: kotlinx.coroutines.channels.SendChannel<ClaimedExecution>,
        emptyPollDelayMillis: Long,
        permits: ClaimPermits,
    ) {
        var nextFullSweepAt = 0L
        var backoffMillis = 0L
        var lastWarnAt = 0L
        while (polling.get() && currentCoroutineContext().isActive) {
            try {
                val now = System.currentTimeMillis()
                val owned = assignedShards(allocator.ownedShards(), claimerIndex)
                val shards = when {
                    owned.isEmpty() -> emptyList()
                    now >= nextFullSweepAt -> {
                        nextFullSweepAt = now + fullSweepIntervalMillis
                        owned
                    }
                    else -> dueShards(now, owned)
                }
                lastProgressAtMillis.set(System.currentTimeMillis())
                backoffMillis = 0

                var claimedAny = false
                if (shards.isNotEmpty()) {
                    // Drop the marks before claiming: anything enqueued from here on marks its shard
                    // again, and whatever a claim leaves behind is re-marked from its result.
                    redis.withCommands { it.zrem(dueKey, shards.map { s -> s.toString().toByteArray() }) }
                    val marks = ArrayList<String>(shards.size * 2)
                    // Shards from [from] on were unmarked above but will not be visited in this
                    // pass (stopping, or a failure): mark them due again right away.
                    fun remarkFrom(from: Int) {
                        val at = System.currentTimeMillis().toString()
                        for (j in from until shards.size) { marks += shards[j].toString(); marks += at }
                    }
                    for ((i, shardId) in shards.withIndex()) {
                        val take = if (polling.get()) {
                            permits.acquireUpTo(claimLimit, active = { polling.get() }) {
                                lastProgressAtMillis.set(System.currentTimeMillis())
                            }
                        } else 0
                        if (take == 0) {
                            remarkFrom(i)
                            break
                        }
                        val (nextAt, items) = try {
                            claim(shardId, take)
                        } catch (ex: Exception) {
                            permits.release(take)
                            remarkFrom(i)
                            runCatching { flushMarks(marks) }
                            throw ex
                        }
                        permits.release(take - items.size)
                        if (nextAt != null) {
                            marks += shardId.toString(); marks += nextAt
                        }
                        if (items.isNotEmpty()) {
                            claimedAny = true
                            items.forEach { buffer.send(it) }
                        }
                    }
                    flushMarks(marks)
                }

                if (!claimedAny && polling.get()) {
                    delay(emptyPollDelayMillis.milliseconds)
                }
            } catch (ex: CancellationException) {
                throw ex
            } catch (ex: Exception) {
                metrics.recordRedisError("claim")
                val now = System.currentTimeMillis()
                if (now - lastWarnAt >= WARN_INTERVAL_MILLIS) {
                    lastWarnAt = now
                    logger.warn("claim poll failed; retrying", kv("claimer", claimerIndex), kv("backoffMillis", backoffMillis), ex)
                }
                // Marks may have been lost mid-pass: the next pass visits every owned shard.
                nextFullSweepAt = 0
                backoffMillis = if (backoffMillis == 0L) MIN_BACKOFF_MILLIS else min(backoffMillis * 2, MAX_BACKOFF_MILLIS)
                delay(backoffMillis.milliseconds)
            }
        }
    }

    private suspend fun dueShards(now: Long, owned: List<Int>): List<Int> {
        val due = redis.withCommands { it.zrangebyscore(dueKey, Double.NEGATIVE_INFINITY, now.toDouble()) }
        if (due.isEmpty()) return emptyList()
        val ownedSet = owned.toHashSet()
        return due.mapNotNull { it.decodeToString().toIntOrNull() }.filter { it in ownedSet }
    }

    private suspend fun flushMarks(marks: MutableList<String>) {
        if (marks.isEmpty()) return
        redis.withCommands { DueIndex.mark(it, dueKey, marks) }
        marks.clear()
    }

    override fun stopPolling() {
        polling.set(false)
    }

    override suspend fun ack(inFlight: ClaimedExecution) {
        val claim = liveClaims.remove(inFlight.claimId)
        val key = keyspace.queueInFlightKey(inFlight.shardId).encodeToByteArray()
        redis.withCommands { commands ->
            if (claim == null) {
                commands.zrem(key, inFlight.payload)
            } else {
                ackScript.run<Long>(commands, ScriptOutputType.INTEGER, arrayOf(key), inFlight.payload, claim.lease.deadlineMillis.toString().toByteArray())
            }
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
     * A claim found expired or taken over is marked lost, which fences the worker holding it.
     * Must run until the processor has drained (see RuntimeBootstrap.stop).
     */
    suspend fun runClaimHeartbeat() {
        val interval = (processingTimeoutMillis / 3).coerceAtLeast(1)
        while (currentCoroutineContext().isActive) {
            delay(interval.milliseconds)
            try {
                renewDueClaims(interval)
            } catch (ex: CancellationException) {
                throw ex
            } catch (ex: Exception) {
                metrics.recordRedisError("claim_heartbeat")
                logger.warn("claim heartbeat failed", ex)
            }
        }
    }

    private suspend fun renewDueClaims(interval: Long) {
        val now = System.currentTimeMillis()
        val due = liveClaims.values.filter { now - it.renewedAtMillis >= interval }
        if (due.isEmpty()) return
        val deadline = now + processingTimeoutMillis
        for ((shardId, claims) in due.groupBy { it.shardId }) {
            for (batch in claims.chunked(RENEW_BATCH_SIZE)) {
                // Past its deadline locally: the claim may already belong to someone else.
                val (expired, held) = batch.partition { !it.lease.isHeld(now) }
                expired.forEach { it.lease.lost = true }
                if (held.isEmpty()) continue
                val args = ArrayList<ByteArray>(1 + held.size * 2)
                args += deadline.toString().toByteArray()
                held.forEach { args += it.payload; args += it.lease.deadlineMillis.toString().toByteArray() }
                val keys = arrayOf(keyspace.queueInFlightKey(shardId).encodeToByteArray())
                val lost = redis.withCommands { commands ->
                    renewClaimsScript.run<List<Long>>(commands, ScriptOutputType.MULTI, keys, *args.toTypedArray())
                }.orEmpty().map { it.toInt() - 1 }.toSet()
                held.forEachIndexed { i, claim ->
                    if (i in lost) {
                        claim.lease.lost = true
                    } else {
                        claim.lease.deadlineMillis = deadline
                        claim.renewedAtMillis = now
                    }
                }
                if (expired.isNotEmpty() || lost.isNotEmpty()) {
                    // Deadline passed before this renewal (e.g. a long GC pause or Redis outage):
                    // those items were already recovered and may run again (at-least-once).
                    logger.warn("claims expired before renewal", kv("shardId", shardId), kv("lost", expired.size + lost.size))
                }
            }
        }
    }

    private suspend fun claim(shardId: Int, limit: Int): Pair<String?, List<ClaimedExecution>> {
        val now = System.currentTimeMillis()
        val deadline = now + processingTimeoutMillis
        val readyKey = keyspace.queueReadyKey(shardId).encodeToByteArray()
        val inFlightKey = keyspace.queueInFlightKey(shardId).encodeToByteArray()

        val result = redis.withCommands { commands ->
            metrics.recordRedisClaimScan()
            claimScript.run<List<ByteArray>>(
                commands,
                ScriptOutputType.MULTI,
                arrayOf(readyKey, inFlightKey),
                now.toString().toByteArray(),
                limit.toString().toByteArray(),
                deadline.toString().toByteArray(),
            )
        }.orEmpty()
        val nextAt = result.firstOrNull()?.decodeToString()?.takeIf { it.isNotEmpty() }
        val items = result.drop(1).map { payload ->
            val execution = msgPack.decodeFromByteArray(SagaExecution.serializer(), payload)
            metrics.recordDequeued(execution)
            val claimId = claimIds.incrementAndGet()
            val lease = ClaimLease(deadline, leaseMarginMillis)
            liveClaims[claimId] = LiveClaim(shardId, payload, lease, now)
            ClaimedExecution(execution, payload, shardId, claimId, lease)
        }
        return nextAt to items
    }

    private fun assignedShards(
        ownedShards: List<Int>,
        claimerIndex: Int,
    ): List<Int> = ownedShards.filterIndexed { index, _ -> index % effectiveClaimerCount == claimerIndex }

    private companion object {
        /** Members per renew script call, bounding the size of a single EVAL. */
        const val RENEW_BATCH_SIZE = 100
        const val MIN_BACKOFF_MILLIS = 100L
        const val MAX_BACKOFF_MILLIS = 5_000L
        const val WARN_INTERVAL_MILLIS = 10_000L
    }
}
