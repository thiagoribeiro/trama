@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaRepositoryStore
import run.trama.saga.UuidAsStringSerializer
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

/**
 * The default store. Execution state, steps, resume points and join barriers are durable in
 * Postgres (inherited from [SagaRepositoryStore]); Redis only adds callback nonces, which are
 * short-lived replay protection. Losing Redis data therefore never loses an execution — see
 * [run.trama.runtime.ExecutionReconciler].
 */
class RedisSagaExecutionStore(
    private val redis: RedisCommandsProvider,
    private val repository: SagaRepository,
    private val keyspace: RedisShardKeyspace,
) : SagaRepositoryStore(repository) {
    private val logger = LoggerFactory.getLogger(RedisSagaExecutionStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Fast replay rejection (409) for callbacks. Best effort: when Redis is unavailable the callback
     * still goes through, since consuming the waiting state in Postgres is what lets a callback
     * resume an execution only once.
     */
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean {
        val key = "saga:nonce:$nonce".toByteArray()
        val value = "1".toByteArray()
        return try {
            redis.withCommands { commands -> commands.setNx(key, value, ttlSeconds) }
        } catch (ex: kotlinx.coroutines.CancellationException) {
            throw ex
        } catch (ex: Exception) {
            logger.warn("callback nonce check skipped: Redis unavailable", ex)
            true
        }
    }

    /**
     * Upgrade path from versions that buffered an execution's meta and step history in Redis and
     * wrote them to Postgres only when it parked or finished: imports whatever is still buffered
     * for [execution] so its history (and the templates that read it) stays complete.
     * Remove once no pre-2.1 execution can still be running.
     */
    override suspend fun adoptLegacy(execution: SagaExecution) {
        val metaKey = keyspace.executionMetaKey(execution.id).toByteArray()
        val stepsKey = keyspace.executionStepsKey(execution.id).toByteArray()
        val (rawMeta, rawSteps) = redis.withCommands { commands ->
            commands.get(metaKey) to commands.lrange(stepsKey, 0, -1)
        }
        if (rawMeta == null && rawSteps.isEmpty()) return
        val steps = rawSteps.mapNotNull { bytes ->
            runCatching { json.decodeFromString(RedisStepEntry.serializer(), bytes.toString(Charsets.UTF_8)) }.getOrNull()
        }
        val meta = rawMeta?.let { runCatching { json.decodeFromString(RedisExecutionMeta.serializer(), it.toString(Charsets.UTF_8)) }.getOrNull() }
        repository.importLegacyState(execution, steps, meta?.failureDescription, meta?.callbackWarning)
        redis.withCommands { commands -> commands.del(metaKey, stepsKey) }
        logger.info("imported pre-2.1 execution state from Redis", net.logstash.logback.argument.StructuredArguments.kv("sagaId", execution.id.toString()))
    }

}

/** Execution meta as buffered in Redis before 2.1 (read only by [RedisSagaExecutionStore.adoptLegacy]). */
@Serializable
data class RedisExecutionMeta(
    @Serializable(with = UuidAsStringSerializer::class)
    val id: UUID,
    val name: String,
    val version: String,
    val definitionJson: String,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    val status: String,
    val failureDescription: String?,
    val callbackWarning: String?,
    val lastFailedStepIndex: Int?,
    val lastFailedPhase: String?,
    /** Default keeps metas written by a previous version (still in Redis during a deploy) readable. */
    val payloadJson: String? = null,
)

/** A step result as buffered in Redis before 2.1. */
@Serializable
data class RedisStepEntry(
    val stepIndex: Int,
    val stepName: String,
    val phase: String,
    val statusCode: Int?,
    val success: Boolean,
    val responseBody: String?,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    @Serializable(with = InstantAsStringSerializer::class)
    val createdAt: Instant,
    @Serializable(with = InstantAsStringSerializer::class)
    val stepStartedAt: Instant = Instant.EPOCH,
)
