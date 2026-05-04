package run.trama.saga.workflow

import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.opentelemetry.api.trace.Span
import run.trama.saga.ExecutionOutcome
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureReason
import run.trama.saga.HttpCall
import run.trama.saga.HttpClientProvider
import run.trama.saga.HttpVerb
import run.trama.saga.PayloadValue
import run.trama.saga.RetryPolicy
import run.trama.saga.RetryState
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.StepCallEntry
import run.trama.saga.StepResult
import run.trama.saga.SagaExecutor
import run.trama.saga.TemplateContextBuilder
import run.trama.saga.TemplateRenderer
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.telemetry.Metrics
import run.trama.telemetry.Tracing
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
import java.time.Instant
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Node-dispatch executor that operates on the [WorkflowDefinition] IR.
 *
 * Backward compat: understands pre-PR2 [ExecutionState.InProgress] where [activeNodeId]
 * is null, deriving the active node from [SagaExecution.currentStepIndex] + the v1 definition.
 */
class WorkflowExecutor(
    private val store: SagaExecutionStore,
    private val renderer: TemplateRenderer,
    private val retryPolicy: RetryPolicy,
    private val enqueuer: SagaEnqueuer,
    private val httpClient: HttpClientProvider,
    private val metrics: Metrics,
    private val maxNodesPerExecution: Int = 25,
    private val sleepMaxChunkMillis: Long = 12 * 3_600_000L,
    private val sleepJitterMillis: Long = 60_000L,
    callbackTokenService: CallbackTokenService? = null,
    callbackUrlFactory: CallbackUrlFactory? = null,
) : SagaExecutor {
    private val logger = LoggerFactory.getLogger(WorkflowExecutor::class.java)
    private val tracer = Tracing.tracer("workflow-executor")
    private val taskHandler = TaskNodeHandler(renderer, httpClient, metrics, callbackTokenService, callbackUrlFactory)

    override suspend fun execute(execution: SagaExecution): ExecutionOutcome {
        return Tracing.withSpan(
            tracer = tracer,
            name = "saga.execute",
            attributes = mapOf(
                "saga.id" to execution.id.toString(),
                "saga.name" to execution.definition.name,
                "saga.version" to execution.definition.version,
            ),
        ) { span ->
            Tracing.withTraceMdc(span, execution.id.toString()) {
                logger.info(
                    "saga execution started",
                    kv("sagaName", execution.definition.name),
                    kv("sagaVersion", execution.definition.version),
                )
            }
            when (val state = execution.state) {
                is ExecutionState.InProgress -> {
                    store.upsertStart(execution)
                    val workflow = resolveWorkflow(execution)
                    val activeNodeId = resolveActiveNodeId(state, execution)
                    val (completed, compStack) = resolveLegacyStacks(state, execution)
                    executeForward(execution, workflow, activeNodeId, completed, compStack, state.retry)
                }
                is ExecutionState.Compensating -> {
                    val workflow = resolveWorkflow(execution)
                    executeCompensating(execution, workflow, state)
                }
                is ExecutionState.Failed -> ExecutionOutcome.FailedFinal
                is ExecutionState.Succeeded -> ExecutionOutcome.Succeeded
                is ExecutionState.WaitingCallback -> {
                    val workflow = resolveWorkflow(execution)
                    executeWaitingCallback(execution, workflow, state)
                }
                is ExecutionState.Sleeping -> {
                    val workflow = resolveWorkflow(execution)
                    executeSleeping(execution, workflow, state)
                }
            }
        }
    }

    // ── Definition resolution ─────────────────────────────────────────────────

    /**
     * Returns the [WorkflowDefinition] for this execution.
     * Uses the v2 definition when present; falls back to normalizing the v1 definition.
     */
    private fun resolveWorkflow(execution: SagaExecution): WorkflowDefinition =
        execution.definitionV2?.let { DefinitionNormalizer.normalize(it) }
            ?: DefinitionNormalizer.normalize(execution.definition)

    // ── Forward execution ─────────────────────────────────────────────────────

    private suspend fun executeForward(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        startNodeId: String,
        initialCompleted: List<String>,
        initialCompStack: List<String>,
        initialRetry: RetryState,
    ): ExecutionOutcome {
        var activeNodeId = startNodeId
        var completedNodes = initialCompleted.toMutableList()
        var compensationStack = initialCompStack.toMutableList()
        var retry = initialRetry
        val pendingCalls = mutableListOf<StepCallEntry>()
        var processed = 0
        // Load once per execution slice — avoids an extra Redis/DB round-trip per node
        val stepResults = store.loadStepResults(execution.id)

        while (true) {
            val node = workflow.nodes[activeNodeId]
                ?: return handleBug(execution, "node '$activeNodeId' not found in workflow")

            val stepIdx = completedNodes.size

            when (node) {
                is TaskNode -> {
                    val httpResult = taskHandler.execute(node, execution, execution.payload, stepResults)

                    store.insertStepResult(
                        sagaId = execution.id,
                        startedAt = execution.startedAt,
                        stepIdx = stepIdx,
                        stepName = node.id,
                        phase = ExecutionPhase.UP,
                        statusCode = httpResult.statusCode,
                        // WaitingForCallback is not a failure — the trigger was accepted
                        success = httpResult.nodeResult !is NodeResult.NodeFailed,
                        responseBody = httpResult.responseBody,
                        stepStartedAt = httpResult.stepStartedAt,
                    )
                    pendingCalls += StepCallEntry(
                        sagaId = execution.id,
                        sagaStartedAt = execution.startedAt,
                        stepName = node.id,
                        phase = ExecutionPhase.UP,
                        attempt = (retry as? RetryState.Applying)?.attempt ?: 0,
                        requestUrl = httpResult.requestUrl,
                        requestBody = httpResult.requestBody,
                        statusCode = httpResult.statusCode,
                        responseBody = httpResult.responseBody,
                        error = httpResult.error,
                        stepStartedAt = httpResult.stepStartedAt,
                    )

                    when (val result = httpResult.nodeResult) {
                        is NodeResult.NodeFailed -> {
                            if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                            return handleForwardFailure(
                                execution, workflow, activeNodeId,
                                completedNodes, compensationStack,
                                retry, result.reason,
                            )
                        }
                        is NodeResult.Advanced -> {
                            retry = RetryState.None
                            completedNodes.add(node.id)
                            if (node.compensation != null) {
                                compensationStack.add(0, node.id)
                            }
                            if (node.next == null) {
                                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                                return finishSuccess(execution, workflow, stepResults)
                            }
                            activeNodeId = node.next
                        }
                        is NodeResult.WaitingForCallback -> {
                            if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                            val updated = execution.copy(
                                state = ExecutionState.WaitingCallback(
                                    nodeId = result.nodeId,
                                    attempt = result.attempt,
                                    deadlineAt = result.deadlineAt,
                                    nonce = result.nonce,
                                    completedNodes = completedNodes.toList(),
                                    compensationStack = compensationStack.toList(),
                                ),
                            )
                            store.saveWaiting(updated, result.signature)
                            val delayMillis = (result.deadlineAt.toEpochMilli() - System.currentTimeMillis()).coerceAtLeast(0)
                            enqueuer.enqueue(updated, delayMillis)
                            metrics.recordCallbackWaitEntered(execution.definition.name, execution.definition.version)
                            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                                logger.info(
                                    "async node waiting for callback",
                                    kv("nodeId", result.nodeId),
                                    kv("deadlineAt", result.deadlineAt.toString()),
                                )
                            }
                            return ExecutionOutcome.Reenqueued
                        }
                    }
                }

                is SwitchNode -> {
                    val switchStartedAt = Instant.now()
                    val evalResult = SwitchNodeHandler.evaluate(node, execution, execution.payload, stepResults)
                    val traceJson = buildSwitchTraceJson(evalResult)
                    store.insertStepResult(
                        sagaId = execution.id,
                        startedAt = execution.startedAt,
                        stepIdx = stepIdx,
                        stepName = node.id,
                        phase = ExecutionPhase.SWITCH,
                        statusCode = null,
                        success = true,
                        responseBody = traceJson,
                        stepStartedAt = switchStartedAt,
                    )
                    retry = RetryState.None
                    activeNodeId = evalResult.targetNodeId
                    metrics.recordSwitchEvaluated(
                        sagaName = execution.definition.name,
                        sagaVersion = execution.definition.version,
                        result = if (evalResult.usedDefault) "default" else "case",
                    )
                    Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                        logger.info(
                            "switch evaluated",
                            kv("nodeId", node.id),
                            kv("target", evalResult.targetNodeId),
                            kv("matchedCase", evalResult.matchedCaseName),
                            kv("usedDefault", evalResult.usedDefault),
                        )
                    }
                }

                is SleepNode -> {
                    if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                    val wakeAt = Instant.now().plusMillis(node.durationMillis)
                    val delay = minOf(node.durationMillis, sleepMaxChunkMillis + sleepJitterMillis)
                    val updated = execution.copy(
                        state = ExecutionState.Sleeping(
                            wakeAt = wakeAt,
                            nextNodeId = node.next,
                            completedNodes = completedNodes.toList(),
                            compensationStack = compensationStack.toList(),
                        ),
                    )
                    store.saveSleeping(updated, wakeAt)
                    enqueuer.enqueue(updated, delay)
                    Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                        logger.info(
                            "saga sleeping",
                            kv("nodeId", node.id),
                            kv("wakeAt", wakeAt.toString()),
                            kv("delayMillis", delay),
                        )
                    }
                    return ExecutionOutcome.Reenqueued
                }
            }

            processed++
            if (processed >= maxNodesPerExecution) {
                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                val updated = execution.copy(
                    state = ExecutionState.InProgress(
                        activeNodeId = activeNodeId,
                        completedNodes = completedNodes,
                        compensationStack = compensationStack,
                    ),
                )
                enqueuer.enqueue(updated, 0)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.info("execution checkpoint scheduled", kv("nextNodeId", activeNodeId))
                }
                return ExecutionOutcome.Reenqueued
            }
        }
    }

    private suspend fun handleForwardFailure(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        failedNodeId: String,
        completedNodes: List<String>,
        compensationStack: List<String>,
        retryState: RetryState,
        reason: FailureReason,
    ): ExecutionOutcome {
        val retryDecision = retryPolicy.next(retryState, workflow.failureHandling)
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
                state = ExecutionState.InProgress(
                    activeNodeId = failedNodeId,
                    completedNodes = completedNodes,
                    compensationStack = compensationStack,
                    retry = RetryState.Applying(retryDecision.attempt, retryDecision.delayMillis),
                ),
            )
            enqueuer.enqueue(updated, retryDecision.delayMillis)
            ExecutionOutcome.Reenqueued
        } else {
            store.updateFailure(execution.id, reason.message, null, null)
            Span.current().addEvent("saga.compensate")
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info("compensation scheduled", kv("failedNodeId", failedNodeId))
            }
            val updated = execution.copy(
                state = ExecutionState.Compensating(
                    compensationStack = compensationStack,
                    completedNodes = completedNodes,
                    failureReason = reason,
                ),
            )
            enqueuer.enqueue(updated, 0)
            ExecutionOutcome.Reenqueued
        }
    }

    private suspend fun finishSuccess(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        stepResults: List<StepResult>,
    ): ExecutionOutcome {
        workflow.onSuccessCallback?.let { callback ->
            val context = TemplateContextBuilder.build(execution, "onSuccessCallback", ExecutionPhase.UP, stepResults, execution.payload)
            val httpResult = executeRawCall("onSuccessCallback", callback, context)
            if (!httpResult.success) {
                val warning = "success callback failed: node=onSuccessCallback status=${httpResult.statusCode ?: httpResult.error}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn("success callback failed", kv("warning", warning))
                }
            }
        }
        store.updateFinal(execution.id, "SUCCEEDED")
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "SUCCEEDED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.Succeeded
    }

    // ── Compensation execution ────────────────────────────────────────────────

    private suspend fun executeCompensating(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        state: ExecutionState.Compensating,
    ): ExecutionOutcome {
        val remaining = state.compensationStack.toMutableList()
        var retry = state.retry
        var processed = 0
        val pendingCalls = mutableListOf<StepCallEntry>()
        // Load once per execution slice
        val stepResults = store.loadStepResults(execution.id)

        while (remaining.isNotEmpty()) {
            val nodeId = remaining.first()
            val node = workflow.nodes[nodeId] as? TaskNode ?: run {
                remaining.removeFirst()
                continue
            }

            val stepIdx = state.completedNodes.size - remaining.size
            val httpResult = taskHandler.compensate(node, execution, execution.payload, stepResults)

            store.insertStepResult(
                sagaId = execution.id,
                startedAt = execution.startedAt,
                stepIdx = stepIdx,
                stepName = node.id,
                phase = ExecutionPhase.DOWN,
                statusCode = httpResult.statusCode,
                success = httpResult.nodeResult is NodeResult.Advanced,
                responseBody = httpResult.responseBody,
                stepStartedAt = httpResult.stepStartedAt,
            )
            pendingCalls += StepCallEntry(
                sagaId = execution.id,
                sagaStartedAt = execution.startedAt,
                stepName = node.id,
                phase = ExecutionPhase.DOWN,
                attempt = (retry as? RetryState.Applying)?.attempt ?: 0,
                requestUrl = httpResult.requestUrl,
                requestBody = httpResult.requestBody,
                statusCode = httpResult.statusCode,
                responseBody = httpResult.responseBody,
                error = httpResult.error,
                stepStartedAt = httpResult.stepStartedAt,
            )

            when (val result = httpResult.nodeResult) {
                is NodeResult.NodeFailed -> {
                    val retryDecision = retryPolicy.next(retry, workflow.failureHandling)
                    return if (retryDecision.shouldRetry) {
                        if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                            logger.info("compensation retry scheduled", kv("nodeId", nodeId))
                        }
                        val updated = execution.copy(
                            state = ExecutionState.Compensating(
                                compensationStack = remaining,
                                completedNodes = state.completedNodes,
                                failureReason = state.failureReason,
                                retry = RetryState.Applying(retryDecision.attempt, retryDecision.delayMillis),
                            ),
                        )
                        enqueuer.enqueue(updated, retryDecision.delayMillis)
                        ExecutionOutcome.Reenqueued
                    } else {
                        if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                        val reason = result.reason.message
                        store.updateFinal(execution.id, "CORRUPTED", reason)
                        metrics.recordSagaDuration(
                            sagaName = execution.definition.name,
                            sagaVersion = execution.definition.version,
                            finalStatus = "CORRUPTED",
                            startedAt = execution.startedAt,
                        )
                        ExecutionOutcome.FailedFinal
                    }
                }
                is NodeResult.Advanced -> {
                    retry = RetryState.None
                    remaining.removeFirst()
                }
                else -> {
                    // compensate() never returns WaitingForCallback; treat as a bug
                    remaining.removeFirst()
                }
            }

            processed++
            if (processed >= maxNodesPerExecution && remaining.isNotEmpty()) {
                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                val updated = execution.copy(
                    state = ExecutionState.Compensating(
                        compensationStack = remaining,
                        completedNodes = state.completedNodes,
                        failureReason = state.failureReason,
                    ),
                )
                enqueuer.enqueue(updated, 0)
                return ExecutionOutcome.Reenqueued
            }
        }

        if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)

        // All compensations complete → fire failure callback then mark FAILED
        workflow.onFailureCallback?.let { callback ->
            val context = TemplateContextBuilder.build(execution, "onFailureCallback", ExecutionPhase.DOWN, stepResults, execution.payload)
            val httpResult = executeRawCall("onFailureCallback", callback, context)
            if (!httpResult.success) {
                val warning = "failure callback failed: node=onFailureCallback status=${httpResult.statusCode ?: httpResult.error}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn("failure callback failed", kv("warning", warning))
                }
            }
        }
        store.updateFinal(execution.id, "FAILED", state.failureReason.message)
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "FAILED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.FailedFinal
    }

    // ── Waiting callback (timeout) ────────────────────────────────────────────

    private suspend fun executeWaitingCallback(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        state: ExecutionState.WaitingCallback,
    ): ExecutionOutcome {
        if (state.deadlineAt.isAfter(Instant.now())) {
            // Delivered early (clock skew or queue re-ordering) — re-schedule for deadline.
            val delayMillis = (state.deadlineAt.toEpochMilli() - System.currentTimeMillis()).coerceAtLeast(1)
            enqueuer.enqueue(execution, delayMillis)
            return ExecutionOutcome.Reenqueued
        }

        val wasWaiting = store.consumeWaiting(execution.id)
        if (wasWaiting == null) {
            // Callback was received and processed before this sentinel fired — nothing to do.
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info("callback timeout sentinel consumed but callback already handled", kv("nodeId", state.nodeId))
            }
            return ExecutionOutcome.Reenqueued
        }

        // Deadline passed without a callback → treat as failure.
        val reason = FailureReason("callback timeout for node ${state.nodeId}")
        store.insertStepResult(
            sagaId = execution.id,
            startedAt = execution.startedAt,
            stepIdx = state.completedNodes.size,
            stepName = state.nodeId,
            phase = ExecutionPhase.CALLBACK,
            statusCode = null,
            success = false,
            responseBody = null,
            stepStartedAt = Instant.now(),
        )
        store.updateFailure(execution.id, reason.message, null, null)
        metrics.recordCallbackTimeout(execution.definition.name, execution.definition.version)
        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.warn("callback timeout", kv("nodeId", state.nodeId), kv("attempt", state.attempt))
        }
        return handleForwardFailure(
            execution, workflow,
            failedNodeId = state.nodeId,
            completedNodes = state.completedNodes,
            compensationStack = state.compensationStack,
            retryState = RetryState.Applying(state.attempt, 0),
            reason = reason,
        )
    }

    // ── Sleep execution ───────────────────────────────────────────────────────

    private suspend fun executeSleeping(
        execution: SagaExecution,
        workflow: WorkflowDefinition,
        state: ExecutionState.Sleeping,
    ): ExecutionOutcome {
        val now = Instant.now()
        if (state.wakeAt.isAfter(now)) {
            // Not yet time to wake — check whether the saga:sleep sentinel still exists.
            // If it's gone, the wake endpoint already fired a fresh InProgress execution;
            // this queue item is stale → ACK and discard.
            val entry = store.peekSleeping(execution.id)
            if (entry == null) {
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.info("stale sleeping queue item discarded (already woken)", kv("sagaId", execution.id.toString()))
                }
                return ExecutionOutcome.Reenqueued
            }
            // Re-enqueue with the next chunk delay.
            val remaining = state.wakeAt.toEpochMilli() - now.toEpochMilli()
            val delay = minOf(remaining, sleepMaxChunkMillis + sleepJitterMillis)
            enqueuer.enqueue(execution, delay)
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info(
                    "saga still sleeping, re-enqueued",
                    kv("wakeAt", state.wakeAt.toString()),
                    kv("delayMillis", delay),
                )
            }
            return ExecutionOutcome.Reenqueued
        }

        // wakeAt has passed — advance to next node.
        // Clean up the sentinel key (if still present; wake endpoint may have already removed it).
        store.consumeSleeping(execution.id)
        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.info("saga waking up", kv("nextNodeId", state.nextNodeId))
        }
        return if (state.nextNodeId == null) {
            // Sleep was the terminal node.
            finishSuccess(execution, workflow, store.loadStepResults(execution.id))
        } else {
            val updated = execution.copy(
                state = ExecutionState.InProgress(
                    activeNodeId = state.nextNodeId,
                    completedNodes = state.completedNodes,
                    compensationStack = state.compensationStack,
                ),
            )
            // Update Postgres back to IN_PROGRESS before re-entering executeForward
            store.upsertStart(updated)
            executeForward(updated, workflow, state.nextNodeId, state.completedNodes, state.compensationStack, RetryState.None)
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Resolves the active node id from [InProgress] state.
     * Legacy (pre-PR2) executions have [InProgress.activeNodeId] == null;
     * we derive the node id from [SagaExecution.currentStepIndex].
     */
    private fun resolveActiveNodeId(state: ExecutionState.InProgress, execution: SagaExecution): String {
        if (state.activeNodeId != null) return state.activeNodeId
        val steps = execution.definition.steps
        val idx = execution.currentStepIndex.coerceIn(0, steps.lastIndex)
        return steps[idx].name
    }

    /**
     * For legacy executions, rebuilds completedNodes and compensationStack from
     * [SagaExecution.currentStepIndex] + definition steps.
     */
    private fun resolveLegacyStacks(
        state: ExecutionState.InProgress,
        execution: SagaExecution,
    ): Pair<List<String>, List<String>> {
        if (state.activeNodeId != null) {
            return state.completedNodes to state.compensationStack
        }
        val steps = execution.definition.steps
        val idx = execution.currentStepIndex.coerceIn(0, steps.lastIndex)
        val completed = steps.take(idx).map { it.name }
        val compStack = steps.take(idx).reversed().filter { it.down.url.value.isNotBlank() }.map { it.name }
        return completed to compStack
    }

    private suspend fun handleBug(execution: SagaExecution, msg: String): ExecutionOutcome {
        logger.error("workflow bug: $msg", kv("sagaId", execution.id.toString()))
        store.updateFinal(execution.id, "CORRUPTED", msg)
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "CORRUPTED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.FailedFinal
    }

    private fun buildSwitchTraceJson(evalResult: SwitchNodeHandler.EvaluationResult): String {
        val obj = buildJsonObject {
            put("target", evalResult.targetNodeId)
            if (evalResult.matchedCaseName != null) {
                put("matchedCase", evalResult.matchedCaseName)
            } else {
                put("matchedCase", JsonNull)
            }
            put("usedDefault", evalResult.usedDefault)
        }
        return kotlinx.serialization.json.Json.encodeToString(JsonObject.serializer(), obj)
    }

    private data class RawCallResult(val success: Boolean, val statusCode: Int?, val error: String?)

    private suspend fun executeRawCall(
        name: String,
        call: HttpCall,
        context: Map<String, Any?>,
    ): RawCallResult {
        val url = renderer.render(call.url, context)
        return try {
            val response = httpClient.client.request(url) {
                method = call.verb.toKtorMethod()
                call.headers.forEach { (k, v) -> header(k, renderer.render(v, context)) }
                call.body?.let { setBody(renderer.render(it, context)) }
            }
            RawCallResult(
                success = response.status.value in call.successStatusCodes,
                statusCode = response.status.value,
                error = null,
            )
        } catch (ex: Exception) {
            RawCallResult(success = false, statusCode = null, error = ex.message)
        }
    }

}
