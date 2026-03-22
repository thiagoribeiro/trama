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
import run.trama.saga.WaitingInfo
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
        val id1 = UUID.randomUUID()
        val id2 = UUID.randomUUID()
        val execution1 = makeExecution(id1, "pay")
        val execution2 = makeExecution(id2, "notify")
        val enqueued = mutableListOf<SagaExecution>()

        val scanner = CallbackTimeoutScanner(
            repository = FakeRepository(expiredIds = listOf(id1, id2), executions = mapOf(id1 to execution1, id2 to execution2)),
            enqueuer = FakeEnqueuer(enqueued),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true),
        )

        val count = scanner.scan()

        assertEquals(2, count)
        assertEquals(setOf(id1, id2), enqueued.map { it.id }.toSet())
    }

    @Test
    fun `scan returns 0 when no expired executions found`() = runBlocking {
        val enqueued = mutableListOf<SagaExecution>()

        val scanner = CallbackTimeoutScanner(
            repository = FakeRepository(expiredIds = emptyList(), executions = emptyMap()),
            enqueuer = FakeEnqueuer(enqueued),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true),
        )

        val count = scanner.scan()

        assertEquals(0, count)
        assertEquals(0, enqueued.size)
    }

    @Test
    fun `scan skips execution when consumeWaitingState returns null (already handled)`() = runBlocking {
        val id = UUID.randomUUID()
        val enqueued = mutableListOf<SagaExecution>()

        val scanner = CallbackTimeoutScanner(
            repository = FakeRepository(expiredIds = listOf(id), executions = emptyMap()), // null consumeWaiting
            enqueuer = FakeEnqueuer(enqueued),
            metrics = metrics,
            config = CallbackTimeoutScannerConfig(enabled = true),
        )

        val count = scanner.scan()

        assertEquals(0, count) // skipped since consumeWaitingState returned null
        assertEquals(0, enqueued.size)
    }

    // ── Fakes ────────────────────────────────────────────────────────────────

    private class FakeRepository(
        private val expiredIds: List<UUID>,
        private val executions: Map<UUID, SagaExecution>,
    ) : CallbackTimeoutRepository {
        override suspend fun findExpiredWaitingExecutions(bufferSeconds: Long, limit: Int): List<UUID> = expiredIds

        override suspend fun consumeWaitingState(executionId: UUID): WaitingInfo? {
            val exec = executions[executionId] ?: return null
            val state = exec.state as? ExecutionState.WaitingCallback ?: return null
            return WaitingInfo(
                nodeId = state.nodeId,
                attempt = state.attempt,
                nonce = state.nonce,
                signature = "fake-sig",
                expiresAt = state.deadlineAt,
                execution = exec,
            )
        }
    }

    private class FakeEnqueuer(private val captured: MutableList<SagaExecution>) : SagaEnqueuer {
        override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
            captured.add(execution)
        }
    }
}
