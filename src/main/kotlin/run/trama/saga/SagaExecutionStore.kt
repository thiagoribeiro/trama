package run.trama.saga

import run.trama.saga.store.SagaRepository

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
    suspend fun loadStepResults(sagaId: java.util.UUID): List<SagaRepository.StepResultForTemplate>
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

    override suspend fun loadStepResults(sagaId: java.util.UUID): List<SagaRepository.StepResultForTemplate> =
        repository.loadStepResultsForTemplate(sagaId)
}
