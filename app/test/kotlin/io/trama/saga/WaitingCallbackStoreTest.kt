package run.trama.saga

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.PostgreSQLContainer
import run.trama.config.DatabaseConfig
import run.trama.config.DatabasePoolConfig
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.SagaRepository

/**
 * Regression coverage for [SagaRepository.consumeWaitingState] against a real (Liquibase-migrated)
 * Postgres. Its split/join sibling, consumeWaitingJoinState, had the identical bug — an
 * `UPDATE ... SET waiting_state = NULL ... RETURNING waiting_state` always returns NULL, since
 * RETURNING reflects the row's POST-update state, not what it held beforehand — caught live on
 * r2d2 (2026-09-21) and fixed in both methods with the same CTE + FOR UPDATE pattern. This method
 * backs the WaitingCallback (async task) resume path under the POSTGRES store backend; under the
 * default REDIS backend it's dormant (RedisSagaExecutionStore.consumeWaiting uses a Redis GETDEL
 * fast path instead), which is why this bug went unnoticed until the split/join path — which has
 * no such fast path — made it unconditionally reachable.
 */
class WaitingCallbackStoreTest {
    @Test
    fun `waiting callback round-trips through real Postgres`() = runBlocking {
        if (!DockerClientFactory.instance().isDockerAvailable) return@runBlocking
        val postgres = PostgreSQLContainer("postgres:15-alpine")
        postgres.start()
        try {
            val db = DatabaseClient(
                DatabaseConfig(
                    host = postgres.host,
                    port = postgres.firstMappedPort,
                    database = postgres.databaseName,
                    user = postgres.username,
                    password = postgres.password,
                    pool = DatabasePoolConfig(),
                ),
                SimpleMeterRegistry(),
            )
            val repository = SagaRepository(db)
            val id = UUID.randomUUID()
            val startedAt = Instant.now()
            val def = SagaDefinition(
                name = "waiting-callback-test",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 0),
                steps = emptyList(),
            )
            val defJson = Json.encodeToString(SagaDefinition.serializer(), def)
            // Mirrors the real call sequence in RedisSagaExecutionStore.saveWaiting: the parent's
            // saga_execution row must already exist before saveWaitingState's UPDATE can affect it.
            repository.upsertExecutionRecord(id, def.name, def.version, defJson, startedAt)

            val execution = SagaExecution(
                definition = def,
                id = id,
                startedAt = startedAt,
                currentStepIndex = 0,
                state = ExecutionState.WaitingCallback(
                    nodeId = "task1",
                    attempt = 1,
                    deadlineAt = startedAt.plusSeconds(60),
                    nonce = "n",
                    completedNodes = emptyList(),
                    compensationStack = emptyList(),
                ),
                payload = emptyMap(),
            )
            val executionJson = Json.encodeToString(SagaExecution.serializer(), execution)
            repository.saveWaitingState(
                executionId = id,
                nodeId = "task1",
                attempt = 1,
                nonce = "n",
                signature = "sig",
                expiresAt = startedAt.plusSeconds(60),
                executionJson = executionJson,
            )

            val consumed = repository.consumeWaitingState(id)
            assertNotNull(consumed, "consumeWaitingState must return the parked WaitingInfo, not null")
            assertEquals(id, consumed.execution.id)
            assertEquals("task1", consumed.nodeId)
            assertEquals("sig", consumed.signature)

            assertNull(repository.consumeWaitingState(id), "consuming twice must be a no-op — already delivered")

            db.close()
        } finally {
            postgres.stop()
        }
    }
}
