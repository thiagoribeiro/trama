@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import io.lettuce.core.ScriptOutputType
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.StepResult
import run.trama.saga.UuidAsStringSerializer
import run.trama.saga.WaitingInfo
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory

class RedisSagaExecutionStore(
    private val redis: RedisCommandsProvider,
    private val repository: SagaRepository,
    private val keyspace: RedisShardKeyspace,
    private val ttlSeconds: Long = 600,
) : SagaExecutionStore {
    private val logger = LoggerFactory.getLogger(RedisSagaExecutionStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Atomically reads and deletes a key in a single round-trip.
     * Equivalent to Redis 6.2+ GETDEL but works on all versions via Lua.
     */
    private val getDelScript = """
        local v = redis.call('GET', KEYS[1])
        if v then redis.call('DEL', KEYS[1]) end
        return v
    """.trimIndent()

    /**
     * Atomically LPUSH + EXPIRE for step result insertion.
     * Avoids two separate round-trips per step result.
     */
    private val lpushExpireScript = """
        redis.call('LPUSH', KEYS[1], ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return 1
    """.trimIndent()

    // SHA1 digests for pre-loaded scripts. Populated by [loadScripts].
    private var getDelScriptSha: String? = null
    private var lpushExpireScriptSha: String? = null

    /** Loads Lua scripts into Redis at startup. Call once before processing begins. */
    suspend fun loadScripts() {
        redis.withCommands { commands ->
            getDelScriptSha = commands.scriptLoad(getDelScript.toByteArray())
            lpushExpireScriptSha = commands.scriptLoad(lpushExpireScript.toByteArray())
        }
    }

    private suspend fun atomicGetDel(key: ByteArray): ByteArray? {
        return redis.withCommands { commands ->
            val sha = getDelScriptSha
            if (sha != null) {
                commands.evalsha<ByteArray>(sha, ScriptOutputType.VALUE, arrayOf(key))
            } else {
                commands.eval<ByteArray>(getDelScript.toByteArray(), ScriptOutputType.VALUE, arrayOf(key))
            }
        }
    }

    override suspend fun upsertStart(execution: SagaExecution) {
        val meta = RedisExecutionMeta(
            id = execution.id,
            name = execution.definition.name,
            version = execution.definition.version,
            definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition),
            startedAt = execution.startedAt,
            status = "IN_PROGRESS",
            failureDescription = null,
            callbackWarning = null,
            lastFailedStepIndex = null,
            lastFailedPhase = null,
        )
        writeMeta(meta)
    }

    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        val existing = readMeta(executionId)
        val meta = existing?.copy(
            status = status,
            failureDescription = failureDescription ?: existing.failureDescription,
        )
        if (meta == null) {
            logger.warn("missing redis meta on finalization for sagaId={}", executionId)
            repository.updateExecutionFinal(executionId, status, failureDescription)
            return
        }
        writeMeta(meta)

        repository.upsertExecutionRecord(
            id = meta.id,
            name = meta.name,
            version = meta.version,
            definitionJson = meta.definitionJson,
            startedAt = meta.startedAt,
        )

        meta.failureDescription?.let {
            repository.updateFailureDescription(
                executionId = meta.id,
                failureDescription = it,
                failedStepIndex = meta.lastFailedStepIndex,
                failedPhase = meta.lastFailedPhase?.let { phase -> ExecutionPhase.valueOf(phase) },
            )
        }
        meta.callbackWarning?.let { repository.updateCallbackWarning(meta.id, it) }

        repository.updateExecutionFinal(meta.id, status, failureDescription)

        val steps = readSteps(meta.id)
        steps.forEach { step ->
            repository.insertStepResult(
                sagaId = meta.id,
                startedAt = step.startedAt,
                stepIdx = step.stepIndex,
                stepNameValue = step.stepName,
                phase = ExecutionPhase.valueOf(step.phase),
                statusCode = step.statusCode,
                success = step.success,
                responseBody = step.responseBody,
            )
        }

        deleteKeys(meta.id)
    }

    override suspend fun updateFailure(
        executionId: UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) {
        val current = readMeta(executionId)
        if (current == null) {
            logger.warn("missing redis meta on failure update for sagaId={}", executionId)
            return
        }
        writeMeta(
            current.copy(
                failureDescription = failureDescription,
                lastFailedStepIndex = failedStepIndex,
                lastFailedPhase = failedPhase?.name,
            )
        )
    }

    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {
        val current = readMeta(executionId)
        if (current == null) {
            logger.warn("missing redis meta on callback warning for sagaId={}", executionId)
            return
        }
        writeMeta(current.copy(callbackWarning = warning))
    }

    override suspend fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
    ) {
        val entry = RedisStepEntry(
            stepIndex = stepIdx,
            stepName = stepName,
            phase = phase.name,
            statusCode = statusCode,
            success = success,
            responseBody = responseBody,
            startedAt = startedAt,
            createdAt = Instant.now(),
        )

        val key = stepsKey(sagaId).toByteArray()
        val payload = json.encodeToString(RedisStepEntry.serializer(), entry).toByteArray()
        redis.withCommands { commands ->
            val sha = lpushExpireScriptSha
            if (sha != null) {
                commands.evalsha<Long>(
                    sha,
                    ScriptOutputType.INTEGER,
                    arrayOf(key),
                    payload,
                    ttlSeconds.toString().toByteArray(),
                )
            } else {
                commands.lpush(key, payload)
                commands.expire(key, ttlSeconds)
            }
        }
        touchMetaTtl(sagaId)
    }

    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> {
        val steps = readSteps(sagaId)
        if (steps.isEmpty()) return emptyList()

        val latestByIndex = linkedMapOf<Int, StepResult>()
        val sorted = steps.sortedByDescending { it.createdAt }
        for (step in sorted) {
            val index = step.stepIndex
            val body = toJsonElement(step.responseBody)
            val current = latestByIndex[index]
            if (current == null) {
                latestByIndex[index] = StepResult(
                    index = index,
                    name = step.stepName,
                    upBody = if (step.phase == ExecutionPhase.UP.name) body else null,
                    downBody = if (step.phase == ExecutionPhase.DOWN.name) body else null,
                )
                continue
            }
            if (step.phase == ExecutionPhase.UP.name && current.upBody == null) {
                latestByIndex[index] = current.copy(upBody = body)
            } else if (step.phase == ExecutionPhase.DOWN.name && current.downBody == null) {
                latestByIndex[index] = current.copy(downBody = body)
            }
        }
        return latestByIndex.values.toList()
    }

    private suspend fun readSteps(sagaId: UUID): List<RedisStepEntry> {
        val key = stepsKey(sagaId)
        return redis.withCommands { commands ->
            val raw = commands.lrange(key.toByteArray(), 0, -1)
            raw.mapNotNull { bytes ->
                runCatching {
                    json.decodeFromString(RedisStepEntry.serializer(), bytes.toString(Charsets.UTF_8))
                }.getOrNull()
            }
        }
    }

    private suspend fun writeMeta(meta: RedisExecutionMeta) {
        val key = metaKey(meta.id)
        val payload = json.encodeToString(RedisExecutionMeta.serializer(), meta).toByteArray()
        redis.withCommands { commands ->
            commands.set(key.toByteArray(), payload)
            commands.expire(key.toByteArray(), ttlSeconds)
        }
    }

    private suspend fun readMeta(executionId: UUID): RedisExecutionMeta? {
        val key = metaKey(executionId)
        return redis.withCommands { commands ->
            val raw = commands.get(key.toByteArray()) ?: return@withCommands null
            runCatching {
                json.decodeFromString(RedisExecutionMeta.serializer(), raw.toString(Charsets.UTF_8))
            }.getOrNull()
        }
    }

    private suspend fun touchMetaTtl(executionId: UUID) {
        val key = metaKey(executionId)
        redis.withCommands { commands ->
            commands.expire(key.toByteArray(), ttlSeconds)
        }
    }

    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {
        val state = execution.state as? ExecutionState.WaitingCallback ?: return
        val executionJson = json.encodeToString(SagaExecution.serializer(), execution)
        val entry = RedisWaitingEntry(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = executionJson,
        )
        val key = waitingKey(execution.id).toByteArray()
        val value = json.encodeToString(RedisWaitingEntry.serializer(), entry).toByteArray()
        val ttl = (state.deadlineAt.epochSecond - Instant.now().epochSecond + 120).coerceAtLeast(60)
        redis.withCommands { commands ->
            commands.set(key, value)
            commands.expire(key, ttl)
        }

        // Write to Postgres so the status endpoint can surface WAITING_CALLBACK
        // and the callback timeout scanner can find timed-out executions.
        val definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition)
        repository.upsertExecutionRecord(execution.id, execution.definition.name, execution.definition.version, definitionJson, execution.startedAt)
        repository.saveWaitingState(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = executionJson,
        )
    }

    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? {
        val key = waitingKey(executionId).toByteArray()
        val raw = atomicGetDel(key) ?: return null
        return runCatching {
            val entry = json.decodeFromString(RedisWaitingEntry.serializer(), raw.toString(Charsets.UTF_8))
            val execution = json.decodeFromString(SagaExecution.serializer(), entry.executionJson)
            WaitingInfo(
                nodeId = entry.nodeId,
                attempt = entry.attempt,
                nonce = entry.nonce,
                signature = entry.signature,
                expiresAt = entry.expiresAt,
                execution = execution,
            )
        }.getOrNull()
    }

    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean {
        val key = "saga:nonce:$nonce".toByteArray()
        val value = "1".toByteArray()
        return redis.withCommands { commands ->
            commands.setNx(key, value, ttlSeconds)
        }
    }

    private suspend fun deleteKeys(executionId: UUID) {
        val meta = metaKey(executionId).toByteArray()
        val steps = stepsKey(executionId).toByteArray()
        redis.withCommands { commands ->
            commands.del(meta, steps)
        }
    }

    private fun metaKey(executionId: UUID): String = keyspace.executionMetaKey(executionId)
    private fun stepsKey(executionId: UUID): String = keyspace.executionStepsKey(executionId)
    private fun waitingKey(executionId: UUID): String = keyspace.waitingKey(executionId)

    private fun toJsonElement(raw: String?): JsonElement? {
        if (raw == null) return null
        return try {
            json.parseToJsonElement(raw)
        } catch (_: Exception) {
            JsonPrimitive(raw)
        }.let { element -> if (element is JsonNull) null else element }
    }
}

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
)

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
)

@Serializable
data class RedisWaitingEntry(
    @Serializable(with = UuidAsStringSerializer::class)
    val executionId: UUID,
    val nodeId: String,
    val attempt: Int,
    val nonce: String,
    val signature: String,
    @Serializable(with = InstantAsStringSerializer::class)
    val expiresAt: Instant,
    /** Full [SagaExecution] JSON (with WaitingCallback state) for re-enqueueing on valid callback. */
    val executionJson: String,
)
