package run.trama.saga.redis

import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb
import run.trama.saga.store.SagaRepository

/**
 * The Redis store keeps an execution's meta and step history under a short idle TTL. A saga that
 * parks (sleep, async callback, retry backoff) for longer than that TTL must not lose them: they
 * feed {{nodes.*}} templates after resuming and the /steps history written at finalization.
 */
class RedisExecutionRetentionTest {
    private lateinit var repo: SagaRepository
    private lateinit var store: RedisSagaExecutionStore

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        repo = SagaRepository(IntegrationDb.client)
        // 1s idle TTL stands in for the production 600s.
        store = RedisSagaExecutionStore(IntegrationDb.redis, repo, RedisShardKeyspace("it-retain-${UUID.randomUUID()}", 64), ttlSeconds = 1)
    }

    private fun execution(state: ExecutionState) = SagaExecution(
        definition = SagaDefinition("retain-${UUID.randomUUID()}", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = state,
    )

    private suspend fun startWithOneStep(exec: SagaExecution) {
        store.upsertStart(exec)
        store.insertStepResult(exec.id, exec.startedAt, 0, "a", ExecutionPhase.UP, 200, true, """{"id":"A-1"}""", exec.startedAt)
    }

    @Test
    fun `a sleeping execution keeps its step history past the idle TTL`() = runBlocking<Unit> {
        val exec = execution(ExecutionState.InProgress(activeNodeId = "a"))
        startWithOneStep(exec)
        val wakeAt = Instant.now().plusSeconds(3)
        store.saveSleeping(exec.copy(state = ExecutionState.Sleeping(wakeAt, "b", listOf("a"), emptyList())), wakeAt)

        delay(2_500)

        assertEquals(listOf("a"), store.loadStepResults(exec.id).map { it.name })
    }

    @Test
    fun `retainUntil keeps meta and steps through a long retry delay, and finalization persists them`() = runBlocking<Unit> {
        val exec = execution(ExecutionState.InProgress(activeNodeId = "a"))
        startWithOneStep(exec)
        store.retainUntil(exec.id, Instant.now().plusSeconds(3))

        delay(2_500)

        assertEquals(listOf("a"), store.loadStepResults(exec.id).map { it.name })
        store.updateFinal(exec.id, "SUCCEEDED")
        assertEquals("SUCCEEDED", repo.getExecutionStatus(exec.id)?.status)
        assertEquals(listOf("a"), repo.getStepResults(exec.id).map { it.stepName }, "history must reach Postgres")
    }
}
