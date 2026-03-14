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
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.store.SagaRepository
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
        val executor = DefaultSagaExecutor(
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
            state = ExecutionState.InProgress(ExecutionPhase.UP),
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
        val executor = DefaultSagaExecutor(
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
            state = ExecutionState.InProgress(ExecutionPhase.UP),
            payload = emptyMap(),
        )
        val outcome = executor.execute(exec)
        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertTrue(enqueuer.enqueued.isNotEmpty())
    }

    @Test
    fun `re-enqueues after processing five steps`() = runBlocking {
        val store = FakeStore()
        val enqueuer = FakeEnqueuer()
        val engine = MockEngine {
            respond("ok", HttpStatusCode.OK, headersOf("Content-Type" to listOf("text/plain")))
        }
        val http = FakeHttpClientProvider(HttpClient(engine))
        val executor = DefaultSagaExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = DefaultRetryPolicy(),
            enqueuer = enqueuer,
            httpClient = http,
            metrics = Metrics(SimpleMeterRegistry()),
            maxStepsPerExecution = 5,
        )
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "s1",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 1),
                steps = (1..6).map { idx ->
                    SagaStep(
                        name = "step$idx",
                        up = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                        down = HttpCall(TemplateString("http://x"), HttpVerb.GET),
                    )
                },
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(ExecutionPhase.UP),
            payload = emptyMap(),
        )

        val outcome = executor.execute(exec)
        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertEquals(null, store.finalStatus)
        assertEquals(1, enqueuer.enqueued.size)
        assertEquals(5, enqueuer.enqueued.first().currentStepIndex)
        assertTrue(enqueuer.enqueued.first().state is ExecutionState.InProgress)
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
    override fun upsertStart(execution: SagaExecution) {}
    override fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        finalStatus = status
    }
    override fun updateFailure(
        executionId: UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?
    ) {}
    override fun updateCallbackWarning(executionId: UUID, warning: String) {}
    override fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
    ) {}
    override fun loadStepResults(sagaId: UUID): List<SagaRepository.StepResultForTemplate> = emptyList()
}
