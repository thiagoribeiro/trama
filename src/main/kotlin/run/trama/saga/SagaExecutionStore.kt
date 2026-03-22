package run.trama.saga

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import run.trama.saga.store.SagaRepository
import java.time.Instant

/**
 * Result of a single step execution, used for template rendering and switch evaluation.
 */
data class StepResult(
    val index: Int,
    val name: String,
    val upBody: JsonElement?,
    val downBody: JsonElement?,
)

/**
 * Minimal info about a waiting execution, used during callback validation and timeout processing.
 */
data class WaitingInfo(
    val nodeId: String,
    val attempt: Int,
    val nonce: String,
    val signature: String,
    val expiresAt: Instant,
    /** Full execution with [ExecutionState.WaitingCallback] state, for re-enqueueing after callback. */
    val execution: SagaExecution,
)

interface SagaExecutionStore {
    suspend fun upsertStart(execution: SagaExecution)
    suspend fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String? = null)
    suspend fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    )
    suspend fun updateCallbackWarning(executionId: java.util.UUID, warning: String)
    suspend fun insertStepResult(
        sagaId: java.util.UUID,
        startedAt: java.time.Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
    )
    suspend fun loadStepResults(sagaId: java.util.UUID): List<StepResult>

    /**
     * Persists a [WaitingInfo] entry for the given execution so the callback receiver
     * can validate tokens and re-enqueue on valid callback.
     * [execution] must have [ExecutionState.WaitingCallback] state.
     */
    suspend fun saveWaiting(execution: SagaExecution, signature: String)

    /**
     * Atomically loads and deletes the waiting entry for [executionId].
     * Returns null when no waiting entry exists (callback already processed or never stored).
     */
    suspend fun consumeWaiting(executionId: java.util.UUID): WaitingInfo?

    /**
     * Claims [nonce] for replay protection.
     * Returns true if the nonce is fresh (first time seen); false if it was already consumed (replay).
     * [ttlSeconds] controls how long the nonce is retained.
     */
    suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean
}

class SagaRepositoryStore(
    private val repository: SagaRepository,
) : SagaExecutionStore {
    override suspend fun upsertStart(execution: SagaExecution) =
        repository.upsertExecutionStart(execution)

    override suspend fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String?) =
        repository.updateExecutionFinal(executionId, status, failureDescription)

    override suspend fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) = repository.updateFailureDescription(executionId, failureDescription, failedStepIndex, failedPhase)

    override suspend fun updateCallbackWarning(executionId: java.util.UUID, warning: String) =
        repository.updateCallbackWarning(executionId, warning)

    override suspend fun insertStepResult(
        sagaId: java.util.UUID,
        startedAt: java.time.Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
    ) = repository.insertStepResult(
        sagaId = sagaId,
        startedAt = startedAt,
        stepIdx = stepIdx,
        stepNameValue = stepName,
        phase = phase,
        statusCode = statusCode,
        success = success,
        responseBody = responseBody,
    )

    override suspend fun loadStepResults(sagaId: java.util.UUID): List<StepResult> =
        repository.loadStepResultsForTemplate(sagaId)

    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {
        val state = execution.state as? ExecutionState.WaitingCallback ?: return
        repository.saveWaitingState(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = Json.encodeToString(SagaExecution.serializer(), execution),
        )
    }

    override suspend fun consumeWaiting(executionId: java.util.UUID): WaitingInfo? =
        repository.consumeWaitingState(executionId)

    // Postgres path does not support distributed nonce dedup; always returns true (fresh).
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
}
