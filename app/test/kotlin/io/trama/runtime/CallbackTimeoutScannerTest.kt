package run.trama.runtime

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import run.trama.runtime.CallbackTimeoutRepository
import run.trama.runtime.CallbackTimeoutScanner
import kotlinx.coroutines.runBlocking
import run.trama.config.CallbackTimeoutScannerConfig
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaStep
import run.trama.saga.TemplateString
import run.trama.telemetry.Metrics
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class CallbackTimeoutScannerTest {
    private val metrics = Metrics(SimpleMeterRegistry())

    private fun makeExecution(id: UUID = UUID.randomUUID(), nodeId: String = "pay"): SagaExecution {
        val def = SagaDefinition(
            name = "order",
            version = "1",
            failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
            steps = listOf(
                SagaStep(
                    name = nodeId,
                    up = HttpCall(url = TemplateString("http://svc/pay"), verb = HttpVerb.POST),
                    down = HttpCall(url = TemplateString(""), verb = HttpVerb.DELETE),
                ),
            ),
        )
        return SagaExecution(
            definition = def,
            id = id,
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.WaitingCallback(
                nodeId = nodeId,
                attempt = 0,
                deadlineAt = Instant.now().minusSeconds(300), // already expired
                nonce = UUID.randomUUID().toString(),
                completedNodes = emptyList(),
                compensationStack = emptyList(),
            ),
            payload = emptyMap(),
        )
    }

    @Test
    fun `scan re-enqueues expired waiting executions`() = runBlocking {
        val execution1 = makeExecution(nodeId = "pay")
        val execution2 = makeExecution(nodeId = "notify")
        val enqueued = mutableListOf<SagaExecution>()

        val scanner = CallbackTimeoutScanner(
            repository = FakeRepository(listOf(execution1, execution2)),
            enqueuer = FakeEnqueuer(enqueued),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true),
        )

        val count = scanner.scan()

        assertEquals(2, count)
        assertEquals(setOf(execution1.id, execution2.id), enqueued.map { it.id }.toSet())
    }

    @Test
    fun `scan returns 0 when no expired executions found`() = runBlocking {
        val enqueued = mutableListOf<SagaExecution>()

        val scanner = CallbackTimeoutScanner(
            repository = FakeRepository(emptyList()),
            enqueuer = FakeEnqueuer(enqueued),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true),
        )

        val count = scanner.scan()

        assertEquals(0, count)
        assertEquals(0, enqueued.size)
    }

    @Test
    fun `scan claims with the configured grace period and batch size`() = runBlocking {
        val repository = FakeRepository(emptyList())
        CallbackTimeoutScanner(
            repository = repository,
            enqueuer = FakeEnqueuer(mutableListOf()),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true, bufferSeconds = 42, batchSize = 7),
        ).scan()

        assertEquals(42L to 7, repository.lastCall)
    }

    // ── Fakes ────────────────────────────────────────────────────────────────

    private class FakeRepository(private val expired: List<SagaExecution>) : CallbackTimeoutRepository {
        var lastCall: Pair<Long, Int>? = null

        override suspend fun claimExpiredCallbackWaits(bufferSeconds: Long, limit: Int): List<SagaExecution> {
            lastCall = bufferSeconds to limit
            return expired
        }
    }

    private class FakeEnqueuer(private val captured: MutableList<SagaExecution>) : SagaEnqueuer {
        override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
            captured.add(execution)
        }
    }
}
