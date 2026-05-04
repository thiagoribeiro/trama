package run.trama.saga.callback

import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureReason
import run.trama.saga.RetryPolicy
import run.trama.saga.RetryState
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.workflow.DefinitionNormalizer
import run.trama.saga.workflow.JsonLogicEvaluator
import run.trama.saga.workflow.TaskNode
import java.time.Instant
import run.trama.telemetry.Metrics
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import run.trama.saga.toAny
import java.util.UUID

/**
 * Handles inbound callbacks for ASYNC task nodes.
 *
 * Validates the token, enforces replay protection via [SagaExecutionStore.claimNonce],
 * evaluates optional json-logic conditions, and re-enqueues the execution as [ExecutionState.InProgress]
 * targeting the next node. Terminal nodes (next == null) are finalized inline.
 */
class CallbackReceiver(
    private val store: SagaExecutionStore,
    private val enqueuer: SagaEnqueuer,
    private val tokenService: CallbackTokenService,
    private val retryPolicy: RetryPolicy,
    private val metrics: Metrics,
) {
    private val logger = LoggerFactory.getLogger(CallbackReceiver::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    sealed class CallbackResult {
        /** Accepted — execution re-enqueued or finalized. */
        object Accepted : CallbackResult()
        /** Rejected with an HTTP status and message. */
        data class Rejected(val httpStatus: Int, val message: String) : CallbackResult()
    }

    /**
     * @param executionId the saga execution id from the path
     * @param nodeId the node id from the path
     * @param token the compact token string from the `X-Callback-Token` header
     * @param rawBody the raw request body (may be empty)
     */
    suspend fun receive(
        executionId: UUID,
        nodeId: String,
        token: String,
        rawBody: String,
    ): CallbackResult {
        // 1. Parse token
        val parsed = tokenService.parseToken(token)
            ?: return CallbackResult.Rejected(401, "invalid callback token")

        // 2. Validate signature and expiry
        val validationResult = tokenService.validate(
            executionId = executionId,
            nodeId = nodeId,
            attempt = parsed.attempt,
            nonce = parsed.nonce,
            expiresAt = parsed.expiresAt,
            incomingSignature = parsed.signature,
        )
        if (validationResult is CallbackValidationResult.Invalid) {
            val reason = when (validationResult.httpStatus) {
                410 -> "expired"
                else -> "invalid_signature"
            }
            metrics.recordCallbackRejected("unknown", "unknown", reason)
            logger.warn(
                "callback rejected",
                kv("executionId", executionId.toString()),
                kv("nodeId", nodeId),
                kv("reason", validationResult.reason),
            )
            return CallbackResult.Rejected(validationResult.httpStatus, validationResult.reason)
        }

        // 3. Nonce replay protection
        val nonceTtl = (parsed.expiresAt.epochSecond - System.currentTimeMillis() / 1000 + 120).coerceAtLeast(60)
        val fresh = store.claimNonce(parsed.nonce, nonceTtl)
        if (!fresh) {
            metrics.recordCallbackRejected("unknown", "unknown", "replay")
            logger.warn(
                "callback replay detected",
                kv("executionId", executionId.toString()),
                kv("nodeId", nodeId),
                kv("nonce", parsed.nonce),
            )
            return CallbackResult.Rejected(409, "duplicate callback (nonce already used)")
        }

        // 4. Atomically consume the waiting entry — this also serves as the ZSET sentinel guard:
        //    if the timeout sentinel already fired and WorkflowExecutor consumed the entry first,
        //    consumeWaiting returns null and we treat the callback as too late.
        val waiting = store.consumeWaiting(executionId)
        if (waiting == null) {
            metrics.recordCallbackRejected("unknown", "unknown", "no_waiting_entry")
            logger.warn(
                "callback arrived but no waiting entry found",
                kv("executionId", executionId.toString()),
                kv("nodeId", nodeId),
            )
            return CallbackResult.Rejected(410, "callback expired or already processed")
        }

        // 5. Verify attempt number matches (guards against stale callbacks from a prior attempt)
        if (waiting.attempt != parsed.attempt) {
            metrics.recordCallbackRejected(
                waiting.execution.definition.name,
                waiting.execution.definition.version,
                "wrong_attempt",
            )
            logger.warn(
                "callback attempt mismatch",
                kv("executionId", executionId.toString()),
                kv("nodeId", nodeId),
                kv("expected", waiting.attempt),
                kv("received", parsed.attempt),
            )
            return CallbackResult.Rejected(409, "callback attempt mismatch")
        }

        val execution = waiting.execution
        val state = execution.state as? ExecutionState.WaitingCallback
            ?: return CallbackResult.Rejected(500, "unexpected execution state")

        // 6. Evaluate json-logic success/failure conditions
        val workflow = if (execution.definitionV2 != null) {
            DefinitionNormalizer.normalize(execution.definitionV2)
        } else {
            DefinitionNormalizer.normalize(execution.definition)
        }
        val node = workflow.nodes[nodeId] as? TaskNode
            ?: return CallbackResult.Rejected(500, "node '$nodeId' not found in workflow")

        val bodyJson: JsonElement? = rawBody.takeIf { it.isNotBlank() }?.let {
            runCatching { json.parseToJsonElement(it) }.getOrNull()
        }

        val callbackConfig = node.action.callbackConfig
        val evalData = buildEvalContext(execution, bodyJson)

        val isFailure = callbackConfig?.failureWhen?.let { JsonLogicEvaluator.evaluateBool(it, evalData) } ?: false
        val isSuccess = when {
            isFailure -> false
            callbackConfig?.successWhen != null -> JsonLogicEvaluator.evaluateBool(callbackConfig.successWhen, evalData)
            else -> true  // no conditions configured → accept any callback as success
        }

        val callbackOutcome = if (isSuccess) "accepted" else "rejected"
        metrics.recordCallbackReceived(execution.definition.name, execution.definition.version, callbackOutcome)
        logger.info(
            "callback received",
            kv("executionId", executionId.toString()),
            kv("nodeId", nodeId),
            kv("attempt", parsed.attempt),
            kv("isSuccess", isSuccess),
        )

        return if (isSuccess) {
            handleSuccess(execution, state, node, rawBody)
        } else {
            handleFailure(execution, workflow, state, node, rawBody)
        }
    }

    private suspend fun handleSuccess(
        execution: SagaExecution,
        state: ExecutionState.WaitingCallback,
        node: TaskNode,
        rawBody: String,
    ): CallbackResult {
        store.insertStepResult(
            sagaId = execution.id,
            startedAt = execution.startedAt,
            stepIdx = state.completedNodes.size,
            stepName = node.id,
            phase = ExecutionPhase.CALLBACK,
            statusCode = null,
            success = true,
            responseBody = rawBody.takeIf { it.isNotBlank() },
            stepStartedAt = Instant.now(),
        )
        val completedNodes = state.completedNodes + node.id
        val compensationStack = if (node.compensation != null) {
            listOf(node.id) + state.compensationStack
        } else {
            state.compensationStack
        }

        val nextNodeId = node.next
        if (nextNodeId == null) {
            // Terminal node — finalize the saga
            store.updateFinal(execution.id, "SUCCEEDED")
            metrics.recordSagaDuration(
                sagaName = execution.definition.name,
                sagaVersion = execution.definition.version,
                finalStatus = "SUCCEEDED",
                startedAt = execution.startedAt,
            )
            logger.info(
                "saga succeeded via async callback",
                kv("executionId", execution.id.toString()),
                kv("nodeId", node.id),
            )
        } else {
            val updated = execution.copy(
                state = ExecutionState.InProgress(
                    activeNodeId = nextNodeId,
                    completedNodes = completedNodes,
                    compensationStack = compensationStack,
                ),
            )
            enqueuer.enqueue(updated, 0)
            logger.info(
                "execution resumed after async callback",
                kv("executionId", execution.id.toString()),
                kv("nodeId", node.id),
                kv("nextNodeId", nextNodeId),
            )
        }
        return CallbackResult.Accepted
    }

    private suspend fun handleFailure(
        execution: SagaExecution,
        workflow: run.trama.saga.workflow.WorkflowDefinition,
        state: ExecutionState.WaitingCallback,
        node: TaskNode,
        rawBody: String,
    ): CallbackResult {
        store.insertStepResult(
            sagaId = execution.id,
            startedAt = execution.startedAt,
            stepIdx = state.completedNodes.size,
            stepName = node.id,
            phase = ExecutionPhase.CALLBACK,
            statusCode = null,
            success = false,
            responseBody = rawBody.takeIf { it.isNotBlank() },
            stepStartedAt = Instant.now(),
        )
        val reason = FailureReason("callback failure condition matched for node ${node.id}")
        val retryDecision = retryPolicy.next(RetryState.Applying(state.attempt, 0), workflow.failureHandling)
        if (retryDecision.shouldRetry) {
            val updated = execution.copy(
                state = ExecutionState.InProgress(
                    activeNodeId = node.id,
                    completedNodes = state.completedNodes,
                    compensationStack = state.compensationStack,
                    retry = RetryState.Applying(retryDecision.attempt, retryDecision.delayMillis),
                ),
            )
            enqueuer.enqueue(updated, retryDecision.delayMillis)
            logger.info(
                "async callback failure — retry scheduled",
                kv("executionId", execution.id.toString()),
                kv("nodeId", node.id),
                kv("attempt", retryDecision.attempt),
            )
        } else {
            // Only record failure when we're actually giving up and compensating
            store.updateFailure(execution.id, reason.message, null, null)
            val updated = execution.copy(
                state = ExecutionState.Compensating(
                    compensationStack = state.compensationStack,
                    completedNodes = state.completedNodes,
                    failureReason = reason,
                ),
            )
            enqueuer.enqueue(updated, 0)
            logger.info(
                "async callback failure — compensation scheduled",
                kv("executionId", execution.id.toString()),
                kv("nodeId", node.id),
            )
        }
        return CallbackResult.Accepted  // HTTP 202 — we accepted the callback, even if it triggered failure
    }

    private fun buildEvalContext(execution: SagaExecution, bodyJson: JsonElement?): Map<String, Any?> {
        val inputMap = execution.payload.mapValues { (_, v) -> v.value.toAny() }
        return mapOf(
            "input" to inputMap,
            "callback" to mapOf("body" to bodyJson.toAny()),
        )
    }
}
