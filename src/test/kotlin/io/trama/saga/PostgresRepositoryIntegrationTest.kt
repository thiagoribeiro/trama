package run.trama.saga

import run.trama.config.DatabaseConfig
import run.trama.config.DatabasePoolConfig
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.SagaRepository
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.DockerClientFactory

class PostgresRepositoryIntegrationTest {
    @Test
    fun `persist and fetch saga status`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
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
                )
            )
            db.withConnection { connection ->
                connection.createStatement().use { stmt ->
                    stmt.execute(
                        """
                        create table if not exists saga_execution (
                            id uuid not null,
                            name text not null,
                            version text not null,
                            definition jsonb,
                            status text not null,
                            failure_description text,
                            callback_warning text,
                            last_failed_step_index integer,
                            last_failed_phase text,
                            started_at timestamptz not null,
                            completed_at timestamptz,
                            updated_at timestamptz not null,
                            primary key (id, started_at)
                        );
                        """.trimIndent()
                    )
                    stmt.execute(
                        """
                        create table if not exists saga_step_result (
                            id bigserial not null,
                            saga_id uuid not null,
                            step_index integer not null,
                            step_name text not null,
                            phase text not null,
                            status_code integer,
                            success boolean not null,
                            response_body jsonb,
                            started_at timestamptz not null,
                            created_at timestamptz not null,
                            primary key (id, started_at)
                        );
                        """.trimIndent()
                    )
                }
            }

            val repository = SagaRepository(db)
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
                state = ExecutionState.InProgress(ExecutionPhase.UP),
                payload = emptyMap(),
            )
            repository.upsertExecutionStart(exec)
            repository.updateExecutionFinal(exec.id, "SUCCEEDED", null)

            val status = repository.getExecutionStatus(exec.id)
            assertNotNull(status)
            assertEquals("SUCCEEDED", status.status)

            db.close()
        } finally {
            postgres.stop()
        }
    }
}
