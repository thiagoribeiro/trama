package io.trama.saga

import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpMethod
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.Span
import io.trama.telemetry.Tracing
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv

class DefaultSagaExecutor(
    private val store: SagaExecutionStore,
    private val renderer: TemplateRenderer,
    private val retryPolicy: RetryPolicy,
    private val enqueuer: SagaEnqueuer,
    private val httpClient: HttpClientProvider,
) : SagaExecutor {
    private val logger = LoggerFactory.getLogger(DefaultSagaExecutor::class.java)
    private val tracer = Tracing.tracer("saga-executor")

    override suspend fun execute(execution: SagaExecution): ExecutionOutcome {
        val payload = execution.payload
        return Tracing.withSpan(
            tracer = tracer,
            name = "saga.execute",
            attributes = mapOf(
                "saga.id" to execution.id.toString(),
                "saga.name" to execution.definition.name,
                "saga.version" to execution.definition.version,
            )
        ) { span ->
            Tracing.withTraceMdc(span, execution.id.toString()) {
                logger.info(
                    "saga execution started",
                    kv("sagaName", execution.definition.name),
                    kv("sagaVersion", execution.definition.version),
                )
            }
            val state = execution.state
            when (state) {
                is ExecutionState.InProgress -> {
                    store.upsertStart(execution)
                    executeInProgress(execution, state, payload)
                }
                is ExecutionState.Failed -> ExecutionOutcome.FailedFinal
                is ExecutionState.Succeeded -> ExecutionOutcome.Succeeded
            }
        }
    }

    private suspend fun executeInProgress(
        execution: SagaExecution,
        state: ExecutionState.InProgress,
        payload: Map<String, PayloadValue>,
    ): ExecutionOutcome {
        return when (state.phase) {
            ExecutionPhase.UP -> executeUp(execution, state, payload)
            ExecutionPhase.DOWN -> executeDown(execution, state, payload)
        }
    }

    private suspend fun executeUp(
        execution: SagaExecution,
        state: ExecutionState.InProgress,
        payload: Map<String, PayloadValue>,
    ): ExecutionOutcome {
        var index = execution.currentStepIndex
        var retryState: RetryState = state.retry
        while (index < execution.definition.steps.size) {
            val step = execution.definition.steps[index]
            val result = executeHttpCall(execution, step.name, ExecutionPhase.UP, step.up, payload)
            store.insertStepResult(
                sagaId = execution.id,
                startedAt = execution.startedAt,
                stepIdx = index,
                stepName = step.name,
                phase = ExecutionPhase.UP,
                statusCode = result.statusCode,
                success = result.success,
                responseBody = result.body,
            )
            if (!result.success) {
                return handleFailure(
                    execution,
                    ExecutionPhase.UP,
                    retryState,
                    index,
                    describeFailure(step.name, result),
                    payload,
                )
            }
            retryState = RetryState.None
            index++
        }

        execution.definition.onSuccessCallback?.let { callback ->
            val result = executeHttpCall(execution, "onSuccessCallback", ExecutionPhase.UP, callback, payload)
            if (!result.success) {
                val warning = "success callback failed: ${describeFailure("onSuccessCallback", result)}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn(
                        "success callback failed",
                        kv("warning", warning),
                    )
                }
            }
        }
        store.updateFinal(execution.id, "SUCCEEDED")
        return ExecutionOutcome.Succeeded
    }

    private suspend fun executeDown(
        execution: SagaExecution,
        state: ExecutionState.InProgress,
        payload: Map<String, PayloadValue>,
    ): ExecutionOutcome {
        var index = execution.currentStepIndex
        var retryState: RetryState = state.retry
        while (index >= 0) {
            val step = execution.definition.steps[index]
            val result = executeHttpCall(execution, step.name, ExecutionPhase.DOWN, step.down, payload)
            store.insertStepResult(
                sagaId = execution.id,
                startedAt = execution.startedAt,
                stepIdx = index,
                stepName = step.name,
                phase = ExecutionPhase.DOWN,
                statusCode = result.statusCode,
                success = result.success,
                responseBody = result.body,
            )
            if (!result.success) {
                return handleFailure(
                    execution,
                    ExecutionPhase.DOWN,
                    retryState,
                    index,
                    describeFailure(step.name, result),
                    payload,
                )
            }
            retryState = RetryState.None
            index--
        }

        execution.definition.onFailureCallback?.let { callback ->
            val result = executeHttpCall(execution, "onFailureCallback", ExecutionPhase.DOWN, callback, payload)
            if (!result.success) {
                val warning = "failure callback failed: ${describeFailure("onFailureCallback", result)}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn(
                        "failure callback failed",
                        kv("warning", warning),
                    )
                }
            }
        }
        store.updateFinal(execution.id, "FAILED", "compensation completed")
        return ExecutionOutcome.FailedFinal
    }

    private suspend fun handleFailure(
        execution: SagaExecution,
        phase: ExecutionPhase,
        retryState: RetryState,
        failedIndex: Int,
        failureDescription: String,
        payload: Map<String, PayloadValue>,
    ): ExecutionOutcome {
        val retryDecision = retryPolicy.next(retryState, execution.definition.failureHandling)
        return if (retryDecision.shouldRetry) {
            Span.current().addEvent("saga.retry")
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info(
                    "retry scheduled",
                    kv("delayMillis", retryDecision.delayMillis),
                    kv("attempt", retryDecision.attempt),
                )
            }
            val updated = execution.copy(
                currentStepIndex = failedIndex,
                state = ExecutionState.InProgress(
                    phase = phase,
                    retry = RetryState.Applying(
                        attempt = retryDecision.attempt,
                        nextDelayMillis = retryDecision.delayMillis,
                    ),
                )
            )
            enqueuer.enqueue(updated, retryDecision.delayMillis)
            ExecutionOutcome.Reenqueued
        } else {
            if (phase == ExecutionPhase.UP) {
                store.updateFailure(execution.id, failureDescription, failedIndex, phase)
                val nextIndex = failedIndex - 1
                val updated = execution.copy(
                    currentStepIndex = nextIndex,
                    state = ExecutionState.InProgress(
                        phase = ExecutionPhase.DOWN,
                        retry = RetryState.None,
                    ),
                )
                enqueuer.enqueue(updated, 0)
                Span.current().addEvent("saga.compensate")
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.info(
                        "compensation scheduled",
                        kv("fromIndex", nextIndex),
                    )
                }
                ExecutionOutcome.Reenqueued
            } else {
                execution.definition.onFailureCallback?.let { callback ->
                    val result = executeHttpCall(execution, "onFailureCallback", ExecutionPhase.DOWN, callback, payload)
                    if (!result.success) {
                        val warning = "failure callback failed: ${describeFailure("onFailureCallback", result)}"
                        store.updateCallbackWarning(execution.id, warning)
                        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                            logger.warn(
                                "failure callback failed",
                                kv("warning", warning),
                            )
                        }
                    }
                }
                store.updateFailure(execution.id, failureDescription, failedIndex, phase)
                store.updateFinal(execution.id, "CORRUPTED", failureDescription)
                ExecutionOutcome.FailedFinal
            }
        }
    }

    private suspend fun executeHttpCall(
        execution: SagaExecution,
        stepName: String,
        phase: ExecutionPhase,
        call: HttpCall,
        payload: Map<String, PayloadValue>,
    ): HttpCallResult {
        val context = buildTemplateContext(execution, stepName, phase, payload)
        val url = renderer.render(call.url, context)
        return Tracing.withSpan(
            tracer = tracer,
            name = "saga.step",
            kind = SpanKind.CLIENT,
            attributes = mapOf(
                "saga.id" to execution.id.toString(),
                "saga.step" to stepName,
                "saga.phase" to phase.name,
                "http.method" to call.verb.name,
                "http.url" to url,
            )
        ) { span ->
            Tracing.withTraceMdc(span, execution.id.toString()) {
                logger.info(
                    "calling step",
                    kv("step", stepName),
                    kv("phase", phase.name),
                    kv("url", url),
                    kv("method", call.verb.name),
                )
            }
            try {
                val response = httpClient.client.request(url) {
                    method = call.verb.toHttpMethod()
                    Tracing.injectHeaders { k, v -> header(k, v) }
                    call.headers.forEach { (k, v) ->
                        header(k, renderer.render(v, context))
                    }
                    call.body?.let { setBody(renderer.render(it, context)) }
                }
                val body = response.bodyAsText()
                val result = HttpCallResult(
                    success = response.status.value in call.successStatusCodes,
                    statusCode = response.status.value,
                    body = body,
                )
                Tracing.withTraceMdc(span, execution.id.toString()) {
                    logger.info(
                        "step completed",
                        kv("step", stepName),
                        kv("phase", phase.name),
                        kv("status", response.status.value),
                    )
                }
                result
            } catch (ex: Exception) {
                span.recordException(ex)
                Tracing.withTraceMdc(span, execution.id.toString()) {
                    logger.warn(
                        "step failed",
                        kv("step", stepName),
                        kv("phase", phase.name),
                        kv("error", ex.message ?: "unknown"),
                    )
                }
                HttpCallResult(
                    success = false,
                    statusCode = null,
                    body = null,
                    error = ex.message,
                )
            }
        }
    }

    private fun HttpVerb.toHttpMethod(): HttpMethod =
        when (this) {
            HttpVerb.GET -> HttpMethod.Get
            HttpVerb.POST -> HttpMethod.Post
            HttpVerb.PUT -> HttpMethod.Put
            HttpVerb.PATCH -> HttpMethod.Patch
            HttpVerb.DELETE -> HttpMethod.Delete
            HttpVerb.HEAD -> HttpMethod.Head
            HttpVerb.OPTIONS -> HttpMethod.Options
        }

    private fun buildTemplateContext(
        execution: SagaExecution,
        stepName: String,
        phase: ExecutionPhase,
        payload: Map<String, PayloadValue>,
    ): Map<String, Any?> {
        val steps = store.loadStepResults(execution.id)
        return TemplateContextBuilder.build(execution, stepName, phase, steps, payload)
    }

    private fun describeFailure(stepName: String, result: HttpCallResult): String {
        return when {
            result.statusCode != null -> "step=$stepName status=${result.statusCode}"
            result.error != null -> "step=$stepName error=${result.error}"
            else -> "step=$stepName failed"
        }
    }

    private data class HttpCallResult(
        val success: Boolean,
        val statusCode: Int?,
        val body: String?,
        val error: String? = null,
    )
}
