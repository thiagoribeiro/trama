package run.trama.saga.redis

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb
import run.trama.telemetry.Metrics

/**
 * A pod that claims work and dies before acking must not lose it: once the in-flight claim
 * expires, the expired-claim poller of whichever pod now owns the shard moves it back to ready.
 */
class RedisCrashRecoveryTest {

    @BeforeTest
    fun setUp() = IntegrationDb.assumeDocker()

    private fun consumer(keyspace: RedisShardKeyspace, allocator: RendezvousShardAllocator, timeoutMillis: Long) =
        SagaExecutionRedisConsumer(
            redis = IntegrationDb.redis,
            keyspace = keyspace,
            allocator = allocator,
            batchSize = 10,
            processingTimeoutMillis = timeoutMillis,
            claimerCount = 1,
            metrics = Metrics(SimpleMeterRegistry()),
        )

    @Test
    fun `work claimed by a crashed pod is re-delivered to the new shard owner`() = runBlocking<Unit> {
        val keyspace = RedisShardKeyspace("it-crash-${UUID.randomUUID()}", 4)
        val execution = SagaExecution(
            definition = SagaDefinition("crash", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "a"),
        )
        RedisSagaEnqueuer(IntegrationDb.redis, keyspace).enqueue(execution, 0)

        // pod-a claims the item with a short processing timeout, then "crashes" (never acks).
        val allocA = RendezvousShardAllocator("pod-a", 4).also { it.updatePods(listOf("pod-a")) }
        val podA = consumer(keyspace, allocA, timeoutMillis = 300)
        val chanA = Channel<ClaimedExecution>(1)
        val producerA = launch { podA.runProducer(chanA, emptyPollDelayMillis = 10) }
        val claimedByA = withTimeout(5_000) { chanA.receive() }
        podA.stopPolling()
        producerA.join()
        assertEquals(execution.id, claimedByA.execution.id)

        // pod-b takes over every shard and runs both the claimer and the expired-claim poller.
        val allocB = RendezvousShardAllocator("pod-b", 4).also { it.updatePods(listOf("pod-b")) }
        val podB = consumer(keyspace, allocB, timeoutMillis = 60_000)
        val chanB = Channel<ClaimedExecution>(1)
        val poller = launch { podB.runExpiredRequeuePoller(intervalMillis = 50) }
        val producerB = launch { podB.runProducer(chanB, emptyPollDelayMillis = 10) }

        val redelivered = withTimeout(10_000) { chanB.receive() }
        podB.stopPolling()
        podB.ack(redelivered)
        producerB.join()
        poller.join()

        assertEquals(execution.id, redelivered.execution.id)
    }
}
