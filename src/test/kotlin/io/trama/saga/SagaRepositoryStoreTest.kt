package run.trama.saga

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlin.test.Test

class SagaRepositoryStoreTest {
    @Test
    fun `delegates to repository`() {
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

        verify { repo.updateFailureDescription(sagaId, "fail", 1, ExecutionPhase.UP) }
        verify { repo.updateCallbackWarning(sagaId, "warn") }
        verify { repo.updateExecutionFinal(sagaId, "FAILED", "fail") }
        verify {
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
