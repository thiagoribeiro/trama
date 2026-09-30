package run.trama.saga

import run.trama.saga.store.IntegrationDb
import run.trama.saga.store.SagaRepository
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking

class PostgresRepositoryIntegrationTest {
    @Test
    fun `persist and fetch saga status`() = runBlocking<Unit> {
        // Uses the shared, Liquibase-migrated container: starting a dedicated one per test
        // intermittently failed to launch under parallel load.
        IntegrationDb.assumeDocker()
        val repository = SagaRepository(IntegrationDb.client)
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "test",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = emptyList(),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = java.util.UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = emptyMap(),
        )
        repository.upsertExecutionStart(exec)
        repository.updateExecutionFinal(exec.id, "SUCCEEDED", null)

        val status = repository.getExecutionStatus(exec.id)
        assertNotNull(status)
        assertEquals("SUCCEEDED", status.status)
    }
}
