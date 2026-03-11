package run.trama.saga

import run.trama.saga.store.SagaRepository

interface SagaExecutionStore {
    fun upsertStart(execution: SagaExecution)
    fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String? = null)
    fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    )
    fun updateCallbackWarning(executionId: java.util.UUID, warning: String)
    fun insertStepResult(
        sagaId: java.util.UUID,
        startedAt: java.time.Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
    )
    fun loadStepResults(sagaId: java.util.UUID): List<SagaRepository.StepResultForTemplate>
}

class SagaRepositoryStore(
    private val repository: SagaRepository,
) : SagaExecutionStore {
    override fun upsertStart(execution: SagaExecution) =
        repository.upsertExecutionStart(execution)

    override fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String?) =
        repository.updateExecutionFinal(executionId, status, failureDescription)

    override fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) = repository.updateFailureDescription(executionId, failureDescription, failedStepIndex, failedPhase)

    override fun updateCallbackWarning(executionId: java.util.UUID, warning: String) =
        repository.updateCallbackWarning(executionId, warning)

    override fun insertStepResult(
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

    override fun loadStepResults(sagaId: java.util.UUID): List<SagaRepository.StepResultForTemplate> =
        repository.loadStepResultsForTemplate(sagaId)
}
