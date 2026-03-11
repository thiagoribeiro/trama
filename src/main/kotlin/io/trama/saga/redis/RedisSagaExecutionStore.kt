@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.RetryState
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.UuidAsStringSerializer
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory

class RedisSagaExecutionStore(
    private val redis: RedisCommandsProvider,
    private val repository: SagaRepository,
    private val ttlSeconds: Long = 600,
) : SagaExecutionStore {
    private val keyPrefix = "saga_executions"
    private val logger = LoggerFactory.getLogger(RedisSagaExecutionStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    override fun upsertStart(execution: SagaExecution) {
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

    override fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
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

        val definition = runCatching {
            json.decodeFromString(SagaDefinition.serializer(), meta.definitionJson)
        }.getOrElse { ex ->
            logger.warn("failed to decode definition for sagaId={}", meta.id, ex)
            null
        }

        if (definition != null) {
            val execution = SagaExecution(
                definition = definition,
                id = meta.id,
                startedAt = meta.startedAt,
                currentStepIndex = 0,
                state = ExecutionState.InProgress(ExecutionPhase.UP, RetryState.None),
                payload = emptyMap(),
            )
            repository.upsertExecutionStart(execution)
        }

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

    override fun updateFailure(
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

    override fun updateCallbackWarning(executionId: UUID, warning: String) {
        val current = readMeta(executionId)
        if (current == null) {
            logger.warn("missing redis meta on callback warning for sagaId={}", executionId)
            return
        }
        writeMeta(current.copy(callbackWarning = warning))
    }

    override fun insertStepResult(
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

        val key = stepsKey(sagaId)
        val payload = json.encodeToString(RedisStepEntry.serializer(), entry).toByteArray()
        withCommandsBlocking { commands ->
            commands.lpush(key.toByteArray(), payload)
            commands.expire(key.toByteArray(), ttlSeconds)
        }
        touchMetaTtl(sagaId)
    }

    override fun loadStepResults(sagaId: UUID): List<SagaRepository.StepResultForTemplate> {
        val steps = readSteps(sagaId)
        if (steps.isEmpty()) return emptyList()

        val latestByIndex = linkedMapOf<Int, SagaRepository.StepResultForTemplate>()
        val sorted = steps.sortedByDescending { it.createdAt }
        for (step in sorted) {
            val index = step.stepIndex
            val body = toJsonElement(step.responseBody)
            val current = latestByIndex[index]
            if (current == null) {
                latestByIndex[index] = SagaRepository.StepResultForTemplate(
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

    private fun readSteps(sagaId: UUID): List<RedisStepEntry> {
        val key = stepsKey(sagaId)
        return withCommandsBlocking { commands ->
            val raw = commands.lrange(key.toByteArray(), 0, -1) ?: emptyList()
            raw.mapNotNull { bytes ->
                runCatching {
                    json.decodeFromString(RedisStepEntry.serializer(), bytes.toString(Charsets.UTF_8))
                }.getOrNull()
            }
        }
    }

    private fun writeMeta(meta: RedisExecutionMeta) {
        val key = metaKey(meta.id)
        val payload = json.encodeToString(RedisExecutionMeta.serializer(), meta).toByteArray()
        withCommandsBlocking { commands ->
            commands.set(key.toByteArray(), payload)
            commands.expire(key.toByteArray(), ttlSeconds)
        }
    }

    private fun readMeta(executionId: UUID): RedisExecutionMeta? {
        val key = metaKey(executionId)
        return withCommandsBlocking { commands ->
            val raw = commands.get(key.toByteArray()) ?: return@withCommandsBlocking null
            runCatching {
                json.decodeFromString(RedisExecutionMeta.serializer(), raw.toString(Charsets.UTF_8))
            }.getOrNull()
        }
    }

    private fun touchMetaTtl(executionId: UUID) {
        val key = metaKey(executionId)
        withCommandsBlocking { commands ->
            commands.expire(key.toByteArray(), ttlSeconds)
        }
    }

    private fun deleteKeys(executionId: UUID) {
        val meta = metaKey(executionId).toByteArray()
        val steps = stepsKey(executionId).toByteArray()
        withCommandsBlocking { commands ->
            commands.del(meta, steps)
        }
    }

    private fun metaKey(executionId: UUID): String = "${keyPrefix}:${executionId}"
    private fun stepsKey(executionId: UUID): String = "${keyPrefix}:${executionId}:steps"

    private fun toJsonElement(raw: String?): JsonElement? {
        if (raw == null) return null
        return try {
            json.parseToJsonElement(raw)
        } catch (_: Exception) {
            JsonPrimitive(raw)
        }.let { element -> if (element is JsonNull) null else element }
    }

    private fun <T> withCommandsBlocking(
        block: suspend (io.lettuce.core.api.coroutines.RedisCoroutinesCommands<ByteArray, ByteArray>) -> T
    ): T = runBlocking { redis.withCommands(block) }
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
