package run.trama.saga

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.CallbackConfigDef
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.StepCallEntry
import run.trama.saga.StepResult
import run.trama.saga.WaitingInfo
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.saga.workflow.WorkflowExecutor
import run.trama.telemetry.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

class DefaultSagaExecutorTest {

    @Test
    fun `successful execution stores final status`() = runBlocking {
        val store = FakeStore()
        val enqueuer = FakeEnqueuer()
        val engine = MockEngine {
            respond("ok", HttpStatusCode.OK, headersOf("Content-Type" to listOf("text/plain")))
        }
        val http = FakeHttpClientProvider(HttpClient(engine))
        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = http,
            metrics = Metrics(SimpleMeterRegistry()),
        )
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "s1",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 1),
                steps = listOf(
                    SagaStep(
                        name = "step1",
                        up = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                        down = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                    )
                ),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "step1"),
            payload = mapOf("userId" to PayloadValue(JsonPrimitive("123"))),
        )
        val outcome = executor.execute(exec)
        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals("SUCCEEDED", store.finalStatus)
        assertTrue(enqueuer.enqueued.isEmpty())
    }

    @Test
    fun `retry enqueues when step fails`() = runBlocking {
        val store = FakeStore()
        val enqueuer = FakeEnqueuer()
        val engine = MockEngine {
            respond("fail", HttpStatusCode.InternalServerError)
        }
        val http = FakeHttpClientProvider(HttpClient(engine))
        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = http,
            metrics = Metrics(SimpleMeterRegistry()),
        )
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "s1",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = listOf(
                    SagaStep(
                        name = "step1",
                        up = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                        down = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                    )
                ),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "step1"),
            payload = emptyMap(),
        )
        val outcome = executor.execute(exec)
        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertTrue(enqueuer.enqueued.isNotEmpty())
    }

    @Test
    fun `re-enqueues after processing max nodes per execution`() = runBlocking {
        val store = FakeStore()
        val enqueuer = FakeEnqueuer()
        val engine = MockEngine {
            respond("ok", HttpStatusCode.OK, headersOf("Content-Type" to listOf("text/plain")))
        }
        val http = FakeHttpClientProvider(HttpClient(engine))
        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = http,
            metrics = Metrics(SimpleMeterRegistry()),
            maxNodesPerExecution = 5,
        )
        val steps = (1..6).map { idx ->
            SagaStep(
                name = "step$idx",
                up = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                down = HttpCall(TemplateString("http://x"), HttpVerb.GET),
            )
        }
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "s1",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 1),
                steps = steps,
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "step1"),
            payload = emptyMap(),
        )

        val outcome = executor.execute(exec)
        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertEquals(null, store.finalStatus)
        assertEquals(1, enqueuer.enqueued.size)
        val enqueuedState = enqueuer.enqueued.first().state
        assertIs<ExecutionState.InProgress>(enqueuedState)
        // After 5 nodes (step1..step5), next active node should be step6
        assertEquals("step6", enqueuedState.activeNodeId)
    }

    @Test
    fun `failed step starts compensation after retries exhausted`() = runBlocking {
        val store = FakeStore()
        val enqueuer = FakeEnqueuer()
        // First call (step1 UP) succeeds, second call (step2 UP) always fails
        var callCount = 0
        val engine = MockEngine {
            callCount++
            if (callCount == 1) respond("ok", HttpStatusCode.OK)
            else respond("fail", HttpStatusCode.InternalServerError)
        }
        val http = FakeHttpClientProvider(HttpClient(engine))
        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = http,
            metrics = Metrics(SimpleMeterRegistry()),
        )
        // maxAttempts = 1 → after 1 failure retry is exhausted immediately
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "s1",
                version = "1",
                failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 0),
                steps = listOf(
                    SagaStep("step1", HttpCall(TemplateString("http://x"), HttpVerb.GET), HttpCall(TemplateString("http://x/down"), HttpVerb.DELETE)),
                    SagaStep("step2", HttpCall(TemplateString("http://x"), HttpVerb.GET), HttpCall(TemplateString("http://x/down"), HttpVerb.DELETE)),
                ),
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "step1"),
            payload = emptyMap(),
        )

        // First pass: step1 succeeds, step2 fails → retry enqueued
        val outcome1 = executor.execute(exec)
        assertEquals(ExecutionOutcome.Reenqueued, outcome1)

        // Second pass (retry): same active node, retry state consuming the one allowed attempt
        val retried = enqueuer.enqueued.first()
        enqueuer.enqueued.clear()
        val outcome2 = executor.execute(retried)
        assertEquals(ExecutionOutcome.Reenqueued, outcome2)

        // Should now be in Compensating state
        val compensatingExec = enqueuer.enqueued.firstOrNull()
        assertTrue(compensatingExec != null, "Expected a Compensating execution to be enqueued")
        assertIs<ExecutionState.Compensating>(compensatingExec.state)
    }

    // ── Async node tests ──────────────────────────────────────────────────────

    /** Builds a v2 execution with a single async task node pointing at [url]. */
    private fun asyncExecution(url: String, nextNodeId: String? = null): SagaExecution {
        val nodeId = "pay"
        val nodes = buildList {
            add(NodeDefinition.Task(
                id = nodeId,
                action = NodeActionDef(
                    mode = TaskMode.ASYNC,
                    request = HttpCall(TemplateString(url), HttpVerb.POST),
                    acceptedStatusCodes = setOf(202),
                    callback = CallbackConfigDef(timeoutMillis = 60_000),
                ),
                next = nextNodeId,
            ))
            if (nextNodeId != null) {
                add(NodeDefinition.Task(
                    id = nextNodeId,
                    action = NodeActionDef(
                        mode = TaskMode.SYNC,
                        request = HttpCall(TemplateString(url), HttpVerb.GET),
                    ),
                ))
            }
        }
        val defV2 = SagaDefinitionV2(
            name = "async-saga",
            version = "1",
            failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
            entrypoint = nodeId,
            nodes = nodes,
        )
        return SagaExecution(
            definition = SagaDefinition(
                name = "async-saga",
                version = "1",
                failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
                steps = emptyList(),
            ),
            definitionV2 = defV2,
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "pay"),
            payload = emptyMap(),
        )
    }

    @Test
    fun `async trigger step is recorded as success=true when 202 accepted`() = runBlocking {
        val store = CapturingStore()
        val enqueuer = FakeEnqueuer()
        val engine = MockEngine {
            respond("""{"id":"tx-1"}""", HttpStatusCode.Accepted, headersOf("Content-Type" to listOf("application/json")))
        }
        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = FakeHttpClientProvider(HttpClient(engine)),
            metrics = Metrics(SimpleMeterRegistry()),
            callbackTokenService = CallbackTokenService("test-secret", "test"),
            callbackUrlFactory = CallbackUrlFactory("http://host"),
        )

        executor.execute(asyncExecution("http://svc/pay", nextNodeId = "ship"))

        val upStep = store.stepResults.singleOrNull { it.phase == ExecutionPhase.UP && it.stepName == "pay" }
            ?: error("no UP step result for pay: ${store.stepResults}")
        assertTrue(upStep.success, "async trigger should be recorded as success=true (202 Accepted)")

        val callEntry = store.stepCalls.singleOrNull { it.stepName == "pay" }
            ?: error("no step call recorded for pay")
        assertNotNull(callEntry.requestUrl, "requestUrl must be propagated for async trigger")
        assertEquals(202, callEntry.statusCode)
    }

    @Test
    fun `callback timeout records CALLBACK step result with success=false`() = runBlocking {
        val store = CapturingStore()
        val enqueuer = FakeEnqueuer()
        // Execution is already past the deadline — executeWaitingCallback will fire the timeout path
        val baseExec = asyncExecution("http://svc/pay")
        val timedOutExec = baseExec.copy(
            state = ExecutionState.WaitingCallback(
                nodeId = "pay",
                attempt = 0,
                deadlineAt = Instant.now().minusSeconds(10),  // already expired
                nonce = "stub-nonce",
                completedNodes = emptyList(),
                compensationStack = emptyList(),
            )
        )

        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = FakeHttpClientProvider(HttpClient(MockEngine { respond("", HttpStatusCode.OK) })),
            metrics = Metrics(SimpleMeterRegistry()),
        )

        // consumeWaiting returns the waiting entry so the timeout path is taken
        store.waitingEntry = WaitingInfo(
            nodeId = "pay",
            attempt = 0,
            nonce = "stub-nonce",
            signature = "sig",
            expiresAt = timedOutExec.state.let { (it as ExecutionState.WaitingCallback).deadlineAt },
            execution = timedOutExec,
        )

        executor.execute(timedOutExec)

        val timeoutStep = store.stepResults.singleOrNull { it.phase == ExecutionPhase.CALLBACK }
            ?: error("no CALLBACK step result recorded after timeout: ${store.stepResults}")
        assertTrue(!timeoutStep.success, "timeout should be recorded as success=false")
        assertEquals("pay", timeoutStep.stepName)
    }
}

private class FakeHttpClientProvider(override val client: HttpClient) : HttpClientProvider

private class FakeEnqueuer : SagaEnqueuer {
    val enqueued = mutableListOf<SagaExecution>()
    override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
        enqueued.add(execution)
    }
}

private class FakeStore : SagaExecutionStore {
    var finalStatus: String? = null
    override suspend fun upsertStart(execution: SagaExecution) {}
    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        finalStatus = status
    }
    override suspend fun updateFailure(
        executionId: UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?
    ) {}
    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {}
    override suspend fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: Instant?,
    ) {}
    override suspend fun insertStepCalls(calls: List<StepCallEntry>) {}
    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = emptyList()
    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? = null
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: java.time.Instant) {}
    override suspend fun peekSleeping(executionId: UUID): run.trama.saga.SleepEntry? = null
    override suspend fun consumeSleeping(executionId: UUID): run.trama.saga.SleepEntry? = null
    override suspend fun updateStatus(executionId: UUID, status: String) {}
}

private class CapturingStore : SagaExecutionStore {
    data class RecordedStep(val stepName: String, val phase: ExecutionPhase, val success: Boolean)

    val stepResults = mutableListOf<RecordedStep>()
    val stepCalls = mutableListOf<StepCallEntry>()
    var waitingEntry: WaitingInfo? = null

    override suspend fun upsertStart(execution: SagaExecution) {}
    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {}
    override suspend fun updateFailure(executionId: UUID, failureDescription: String, failedStepIndex: Int?, failedPhase: ExecutionPhase?) {}
    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {}
    override suspend fun insertStepResult(sagaId: UUID, startedAt: Instant, stepIdx: Int, stepName: String, phase: ExecutionPhase, statusCode: Int?, success: Boolean, responseBody: String?, stepStartedAt: Instant?) {
        stepResults += RecordedStep(stepName, phase, success)
    }
    override suspend fun insertStepCalls(calls: List<StepCallEntry>) { stepCalls += calls }
    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = emptyList()
    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? = waitingEntry.also { waitingEntry = null }
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: java.time.Instant) {}
    override suspend fun peekSleeping(executionId: UUID): run.trama.saga.SleepEntry? = null
    override suspend fun consumeSleeping(executionId: UUID): run.trama.saga.SleepEntry? = null
    override suspend fun updateStatus(executionId: UUID, status: String) {}
}
