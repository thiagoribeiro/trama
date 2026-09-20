package run.trama.saga.redis

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import run.trama.config.DatabaseConfig
import run.trama.config.DatabasePoolConfig
import run.trama.config.RedisConfig
import run.trama.config.RedisConsumerConfig
import run.trama.config.RedisPoolConfig
import run.trama.config.RedisQueueConfig
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.JoinBranchLink
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.SagaRepository

/**
 * Verifies the Redis fast path for the join arrival counter (a single atomic INCR — the
 * whole point being no lock is needed) against a *real* Redis + Postgres, including under
 * genuine concurrent arrivals: exactly one of N concurrent branch completions must observe
 * `arrived == expected`, regardless of ordering/timing.
 */
class RedisSplitJoinStoreTest {

    private fun testExecution(id: UUID = UUID.randomUUID()): SagaExecution = SagaExecution(
        definition = SagaDefinition(
            name = "split-join-redis-test",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            steps = emptyList(),
        ),
        id = id,
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.WaitingJoin(
            splitNodeId = "fan-out",
            joinNodeId = "fan-in",
            expectedBranches = 5,
            completedNodes = listOf("fan-out"),
            compensationStack = emptyList(),
        ),
        payload = emptyMap(),
    )

    @Test
    fun `only one of N concurrent arrivals observes the barrier satisfied`() = runBlocking {
        if (!DockerClientFactory.instance().isDockerAvailable) return@runBlocking
        withStores { store ->
            val parent = testExecution()
            val branches = (1..5).map { i -> JoinBranchLink("branch-$i", UUID.randomUUID(), Instant.now()) }
            store.registerJoinBarrier(parent.id, parent.startedAt, "fan-out", "fan-in", branches)

            val results = branches.map { branch ->
                async { store.incrementJoinArrival(parent.id, parent.startedAt, "fan-out") }
            }.awaitAll()

            assertTrue(results.all { it != null }, "every increment must see a registered barrier")
            val satisfiedCount = results.count { it!!.arrived == it.expected }
            assertEquals(1, satisfiedCount, "exactly one concurrent arrival must observe arrived == expected, got: $results")
            assertTrue(results.all { it!!.expected == 5 })
            assertEquals((1..5).toSet(), results.map { it!!.arrived }.toSet(), "arrived counts must be 1..5 with no duplicates/gaps")
        }
    }

    @Test
    fun `getJoinBranches returns what was registered`() = runBlocking {
        if (!DockerClientFactory.instance().isDockerAvailable) return@runBlocking
        withStores { store ->
            val parent = testExecution()
            val branches = listOf(
                JoinBranchLink("branch-a", UUID.randomUUID(), Instant.now()),
                JoinBranchLink("branch-b", UUID.randomUUID(), Instant.now()),
            )
            store.registerJoinBarrier(parent.id, parent.startedAt, "fan-out", "fan-in", branches)

            val fetched = store.getJoinBranches(parent.id, parent.startedAt, "fan-out")
            assertEquals(branches.map { it.branchId }.toSet(), fetched.map { it.branchId }.toSet())
            assertEquals(branches.map { it.childId }.toSet(), fetched.map { it.childId }.toSet())
        }
    }

    @Test
    fun `waiting join round-trips through the redis fast path`() = runBlocking {
        if (!DockerClientFactory.instance().isDockerAvailable) return@runBlocking
        withStores { store ->
            val parent = testExecution()
            store.saveWaitingJoin(parent)

            val consumed = store.consumeWaitingJoin(parent.id)
            assertNotNull(consumed)
            assertEquals(parent.id, consumed.id)
            assertTrue(consumed.state is ExecutionState.WaitingJoin)

            // Consuming again must be a no-op (already delivered) — this is exactly the
            // property finalizeAndNotifyParent relies on to guarantee a single resume.
            assertNull(store.consumeWaitingJoin(parent.id))
        }
    }

    private suspend fun withStores(block: suspend (RedisSagaExecutionStore) -> Unit) {
        val postgres = PostgreSQLContainer("postgres:15-alpine")
        val redisContainer = GenericContainer(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379)
        postgres.start()
        redisContainer.start()
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
            val redisUrl = "redis://${redisContainer.host}:${redisContainer.getMappedPort(6379)}"
            val redis = RedisClientProvider(
                RedisConfig(url = redisUrl, pool = RedisPoolConfig(), queue = RedisQueueConfig(), consumer = RedisConsumerConfig()),
            )
            val keyspace = RedisShardKeyspace("saga:executions", 64)
            val store = RedisSagaExecutionStore(redis, repository, keyspace)
            try {
                block(store)
            } finally {
                redis.close()
                db.close()
            }
        } finally {
            redisContainer.stop()
            postgres.stop()
        }
    }
}
