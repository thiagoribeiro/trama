package run.trama.saga

import io.mockk.coVerify
import io.mockk.mockk
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlinx.coroutines.runBlocking

class SagaRepositoryStoreTest {
    @Test
    fun `delegates to repository`() = runBlocking {
        val repo = mockk<SagaRepository>(relaxed = true)
        val store = SagaRepositoryStore(repo)
        val sagaId = UUID.randomUUID()
        val now = Instant.now()

        store.updateFailure(sagaId, "fail", 1, ExecutionPhase.UP)
        store.updateCallbackWarning(sagaId, "warn")
        store.updateFinal(sagaId, "FAILED", "fail")
        store.insertStepResult(
            sagaId = sagaId,
            startedAt = now,
            stepIdx = 0,
            stepName = "s",
            phase = ExecutionPhase.UP,
            statusCode = 200,
            success = true,
            responseBody = "{}",
        )

        coVerify { repo.updateFailureDescription(sagaId, "fail", 1, ExecutionPhase.UP) }
        coVerify { repo.updateCallbackWarning(sagaId, "warn") }
        coVerify { repo.updateExecutionFinal(sagaId, "FAILED", "fail") }
        coVerify {
            repo.insertStepResult(
                sagaId = sagaId,
                startedAt = now,
                stepIdx = 0,
                stepNameValue = "s",
                phase = ExecutionPhase.UP,
                statusCode = 200,
                success = true,
                responseBody = "{}",
            )
        }
    }
}
