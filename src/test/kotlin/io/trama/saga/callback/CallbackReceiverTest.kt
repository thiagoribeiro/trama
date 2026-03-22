package run.trama.saga.callback

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import run.trama.saga.DefaultRetryPolicy
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.SagaStep
import run.trama.saga.TemplateString
import run.trama.saga.StepResult
import run.trama.saga.WaitingInfo
import run.trama.telemetry.Metrics
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertIs

class CallbackReceiverTest {
    private val secret = "test-secret"
    private val tokenService = CallbackTokenService(secret, "default")
    private val metrics = Metrics(SimpleMeterRegistry())
    private val retryPolicy = DefaultRetryPolicy()
    private val executionId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val nodeId = "payment"

    /** Build a v1-based execution with one or two steps. A second step named [nextNodeName] is added when non-null. */
    private fun makeExecution(nextNodeName: String? = "ship"): SagaExecution {
        val paymentStep = SagaStep(
            name = nodeId,
            up = HttpCall(url = TemplateString("http://svc/pay"), verb = HttpVerb.POST),
            down = HttpCall(url = TemplateString("http://svc/refund"), verb = HttpVerb.DELETE),
        )
        val steps = if (nextNodeName != null) {
            listOf(
                paymentStep,
                SagaStep(
                    name = nextNodeName,
                    up = HttpCall(url = TemplateString("http://svc/ship"), verb = HttpVerb.POST),
                    down = HttpCall(url = TemplateString(""), verb = HttpVerb.DELETE),
                ),
            )
        } else {
            listOf(paymentStep)
        }
        // Name encodes the steps so each variant gets a distinct DefinitionNormalizer cache key.
        val definition = SagaDefinition(
            name = "order-${nextNodeName ?: "terminal"}",
            version = "1",
            failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
            steps = steps,
        )
        return SagaExecution(
            definition = definition,
            id = executionId,
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.WaitingCallback(
                nodeId = nodeId,
                attempt = 0,
                deadlineAt = Instant.now().plusSeconds(60),
                nonce = "stub-nonce",
                completedNodes = emptyList(),
                compensationStack = emptyList(),
            ),
            payload = emptyMap(),
        )
    }

    @Test
    fun `valid callback for non-terminal node re-enqueues as InProgress targeting next node`() = runBlocking {
        val execution = makeExecution(nextNodeName = "ship")
        val meta = tokenService.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val token = tokenService.tokenString(meta)
        val enqueued = mutableListOf<SagaExecution>()
        val receiver = receiver(execution, meta, enqueued)

        val result = receiver.receive(executionId, nodeId, token, "{}")

        assertIs<CallbackReceiver.CallbackResult.Accepted>(result)
        assert(enqueued.size == 1) { "expected one re-enqueue, got ${enqueued.size}" }
        val state = assertIs<ExecutionState.InProgress>(enqueued.first().state)
        assert(state.activeNodeId == "ship") { "expected activeNodeId=ship, got ${state.activeNodeId}" }
    }

    @Test
    fun `valid callback for terminal node finalizes saga without re-enqueueing`() = runBlocking {
        val execution = makeExecution(nextNodeName = null)
        val meta = tokenService.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val token = tokenService.tokenString(meta)
        val enqueued = mutableListOf<SagaExecution>()
        val store = FakeStore(execution = execution, meta = meta)
        val receiver = CallbackReceiver(store, FakeEnqueuer(enqueued), tokenService, retryPolicy, metrics)

        val result = receiver.receive(executionId, nodeId, token, "{}")

        assertIs<CallbackReceiver.CallbackResult.Accepted>(result)
        assert(enqueued.isEmpty()) { "terminal node should not re-enqueue, got ${enqueued.size}" }
        assert(store.finalStatus == "SUCCEEDED") { "expected SUCCEEDED, got ${store.finalStatus}" }
    }

    @Test
    fun `malformed token returns 401`() = runBlocking {
        val execution = makeExecution()
        val meta = tokenService.generate(executionId, nodeId, 0, 60_000)
        val receiver = receiver(execution, meta, mutableListOf())

        val result = receiver.receive(executionId, nodeId, "bad-token", "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 401) { "expected 401, got ${rejected.httpStatus}" }
    }

    @Test
    fun `expired token returns 410`() = runBlocking {
        val execution = makeExecution()
        // Generate a token with negative timeout so it's already expired
        val meta = tokenService.generate(executionId, nodeId, 0, -1_000)
        val token = tokenService.tokenString(meta)
        val receiver = receiver(execution, meta, mutableListOf())

        val result = receiver.receive(executionId, nodeId, token, "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 410) { "expected 410, got ${rejected.httpStatus}" }
    }

    @Test
    fun `wrong signature returns 401`() = runBlocking {
        val execution = makeExecution()
        val meta = tokenService.generate(executionId, nodeId, 0, 60_000)
        val wrongToken = "${meta.nonce}:0:${meta.expiresAt.epochSecond}:bad-sig"
        val receiver = receiver(execution, meta, mutableListOf())

        val result = receiver.receive(executionId, nodeId, wrongToken, "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 401) { "expected 401, got ${rejected.httpStatus}" }
    }

    @Test
    fun `replay nonce returns 409`() = runBlocking {
        val execution = makeExecution()
        val meta = tokenService.generate(executionId, nodeId, 0, 60_000)
        val token = tokenService.tokenString(meta)
        val store = FakeStore(execution = execution, meta = meta, nonceAlreadyClaimed = true)
        val receiver = CallbackReceiver(store, FakeEnqueuer(), tokenService, retryPolicy, metrics)

        val result = receiver.receive(executionId, nodeId, token, "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 409) { "expected 409, got ${rejected.httpStatus}" }
    }

    @Test
    fun `no waiting entry returns 410`() = runBlocking {
        val execution = makeExecution()
        val meta = tokenService.generate(executionId, nodeId, 0, 60_000)
        val token = tokenService.tokenString(meta)
        val store = FakeStore(execution = null, meta = meta)
        val receiver = CallbackReceiver(store, FakeEnqueuer(), tokenService, retryPolicy, metrics)

        val result = receiver.receive(executionId, nodeId, token, "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 410) { "expected 410, got ${rejected.httpStatus}" }
    }

    @Test
    fun `attempt mismatch returns 409`() = runBlocking {
        val execution = makeExecution()
        // Token signed for attempt=1 but waiting entry has attempt=0
        val meta = tokenService.generate(executionId, nodeId, attempt = 1, timeoutMillis = 60_000)
        val token = tokenService.tokenString(meta)
        val store = FakeStore(execution = execution, meta = meta)
        val receiver = CallbackReceiver(store, FakeEnqueuer(), tokenService, retryPolicy, metrics)

        val result = receiver.receive(executionId, nodeId, token, "{}")
        val rejected = assertIs<CallbackReceiver.CallbackResult.Rejected>(result)
        assert(rejected.httpStatus == 409) { "expected 409 attempt mismatch, got ${rejected.httpStatus}" }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun receiver(
        execution: SagaExecution,
        meta: CallbackMeta,
        enqueued: MutableList<SagaExecution>,
    ): CallbackReceiver {
        val store = FakeStore(execution = execution, meta = meta)
        return CallbackReceiver(store, FakeEnqueuer(enqueued), tokenService, retryPolicy, metrics)
    }

    private class FakeStore(
        private val execution: SagaExecution?,
        private val meta: CallbackMeta,
        private val nonceAlreadyClaimed: Boolean = false,
    ) : SagaExecutionStore {
        var finalStatus: String? = null
        private var consumed = false

        override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = !nonceAlreadyClaimed

        override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? {
            if (execution == null || consumed) return null
            consumed = true
            val state = execution.state as? ExecutionState.WaitingCallback ?: return null
            return WaitingInfo(
                nodeId = state.nodeId,
                attempt = state.attempt,
                nonce = state.nonce,
                signature = meta.signature,
                expiresAt = state.deadlineAt,
                execution = execution,
            )
        }

        override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
        override suspend fun upsertStart(execution: SagaExecution) {}
        override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) { finalStatus = status }
        override suspend fun updateFailure(executionId: UUID, failureDescription: String, failedStepIndex: Int?, failedPhase: ExecutionPhase?) {}
        override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {}
        override suspend fun insertStepResult(sagaId: UUID, startedAt: Instant, stepIdx: Int, stepName: String, phase: ExecutionPhase, statusCode: Int?, success: Boolean, responseBody: String?) {}
        override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = emptyList()
    }

    private class FakeEnqueuer(private val captured: MutableList<SagaExecution> = mutableListOf()) : SagaEnqueuer {
        override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) { captured.add(execution) }
    }
}
