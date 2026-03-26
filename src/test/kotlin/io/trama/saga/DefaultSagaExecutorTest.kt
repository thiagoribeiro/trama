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
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.StepResult
import run.trama.saga.WaitingInfo
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
    ) {}
    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = emptyList()
    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? = null
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
}
