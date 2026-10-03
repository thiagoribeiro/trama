package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import kotlin.time.Duration.Companion.milliseconds

/**
 * Moves queued work out of shards that no longer exist. Executions map to `hash(id) % shardCount`,
 * so lowering `redis.sharding.virtualShardCount` leaves items in shards past the new count where
 * no pod claims them. Each pod periodically looks for such queue keys and re-enqueues their items
 * into the shard they map to now, keeping their due time:
 * - ready items are moved right away;
 * - in-flight items only once their claim lease expired: until then a pod still running the old
 *   count may be processing them.
 * A no-op (one SCAN) when the count never changed.
 */
class OrphanedShardMigrator(
    private val redis: RedisCommandsProvider,
    private val keyspace: RedisShardKeyspace,
    private val enqueuer: SagaEnqueuer,
    private val intervalMillis: Long = 60_000,
) {
    private val logger = LoggerFactory.getLogger(OrphanedShardMigrator::class.java)
    private val msgPack = MsgPack()

    suspend fun runLoop() {
        while (true) {
            try {
                migrateOnce()
            } catch (ex: CancellationException) {
                throw ex
            } catch (ex: Exception) {
                logger.warn("orphaned shard migration failed", ex)
            }
            delay(intervalMillis.milliseconds)
        }
    }

    /** Returns how many items were moved. Visible for testing. */
    suspend fun migrateOnce(): Int {
        val orphaned = redis.withCommands { it.scanKeys(keyspace.queueKeysPattern()) }
            .mapNotNull { key -> keyspace.parseQueueKey(key.decodeToString())?.let { key to it } }
            .filter { (_, parsed) -> parsed.first >= keyspace.shardCount }
        if (orphaned.isEmpty()) return 0

        val now = System.currentTimeMillis()
        var moved = 0
        for ((key, parsed) in orphaned) {
            val (shardId, inflight) = parsed
            val members = redis.withCommands { it.zrangeWithScores(key) }
            for ((payload, score) in members) {
                // In flight: the score is the claim deadline. Leave live claims to their owner.
                if (inflight && score > now) continue
                val execution = runCatching { msgPack.decodeFromByteArray(SagaExecution.serializer(), payload) }.getOrNull()
                if (execution != null) {
                    val delayMillis = if (inflight) 0 else (score.toLong() - now).coerceAtLeast(0)
                    enqueuer.enqueue(execution, delayMillis)
                }
                redis.withCommands { it.zrem(key, payload) }
                moved++
            }
            if (members.isNotEmpty()) {
                logger.info("moved queued work out of a removed shard", kv("shard", shardId), kv("inflight", inflight), kv("items", members.size))
            }
        }
        // Their marks in the due index point at shards no claimer visits.
        val dueKey = keyspace.queueDueKey().encodeToByteArray()
        val staleMarks = orphaned.map { it.second.first }.distinct().map { it.toString().toByteArray() }
        redis.withCommands { it.zrem(dueKey, staleMarks) }
        return moved
    }
}
