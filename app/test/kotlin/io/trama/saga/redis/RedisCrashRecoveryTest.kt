package run.trama.saga.redis

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb
import run.trama.telemetry.Metrics

/**
 * In-flight claims: work claimed by a pod that dies must be re-delivered (by the shard owner's
 * normal claim pass), and work that is still being processed must not be (claim heartbeat).
 */
class RedisCrashRecoveryTest {
    private lateinit var keyspace: RedisShardKeyspace

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        keyspace = RedisShardKeyspace("it-claims-${UUID.randomUUID()}", 4)
    }

    private fun consumer(podId: String, timeoutMillis: Long) = SagaExecutionRedisConsumer(
        redis = IntegrationDb.redis,
        keyspace = keyspace,
        allocator = RendezvousShardAllocator(podId, 4).also { it.updatePods(listOf(podId)) },
        batchSize = 10,
        processingTimeoutMillis = timeoutMillis,
        claimerCount = 1,
        metrics = Metrics(SimpleMeterRegistry()),
    )

    private suspend fun enqueueOne(): SagaExecution {
        val execution = SagaExecution(
            definition = SagaDefinition("claims", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "a"),
        )
        RedisSagaEnqueuer(IntegrationDb.redis, keyspace).enqueue(execution, 0)
        return execution
    }

    /** Claims one item with [pod], then stops its producer (the claim stays in flight, unacked). */
    private suspend fun claimOne(pod: SagaExecutionRedisConsumer): ClaimedExecution = kotlinx.coroutines.coroutineScope {
        val channel = Channel<ClaimedExecution>(1)
        val producer = launch { pod.runProducer(channel, emptyPollDelayMillis = 10) }
        val claimed = withTimeout(5_000) { channel.receive() }
        pod.stopPolling()
        producer.join()
        claimed
    }

    /** Runs [pod]'s claim loop for up to [timeoutMs] and returns what it claimed, if anything. */
    private suspend fun tryClaim(pod: SagaExecutionRedisConsumer, timeoutMs: Long): ClaimedExecution? = kotlinx.coroutines.coroutineScope {
        val channel = Channel<ClaimedExecution>(1)
        val producer = launch { pod.runProducer(channel, emptyPollDelayMillis = 10) }
        val claimed = withTimeoutOrNull(timeoutMs) { channel.receive() }
        pod.stopPolling()
        producer.join()
        claimed?.also { pod.ack(it) }
    }

    @Test
    fun `work claimed by a crashed pod is re-delivered by the owner's claim pass`() = runBlocking<Unit> {
        val execution = enqueueOne()
        // pod-a claims with a short lease and "crashes": no ack, no heartbeat.
        assertEquals(execution.id, claimOne(consumer("pod-a", timeoutMillis = 300)).execution.id)

        // pod-b only runs its claimers (there is no separate requeue poller any more).
        val redelivered = tryClaim(consumer("pod-b", timeoutMillis = 60_000), timeoutMs = 5_000)

        assertEquals(execution.id, redelivered?.execution?.id)
    }

    @Test
    fun `the claim heartbeat keeps work that is still being processed from being re-delivered`() = runBlocking<Unit> {
        enqueueOne()
        val podA = consumer("pod-a", timeoutMillis = 600)
        val claimed = claimOne(podA)
        val heartbeat = launch { podA.runClaimHeartbeat() }

        // Well past the 600ms lease: without renewals pod-b would recover the item.
        val stolen = tryClaim(consumer("pod-b", timeoutMillis = 60_000), timeoutMs = 2_000)

        assertNull(stolen, "a live claim must not be re-delivered while its owner keeps renewing it")
        podA.ack(claimed)
        heartbeat.cancel()
    }

    @Test
    fun `a released claim stops being renewed and is re-delivered`() = runBlocking<Unit> {
        val execution = enqueueOne()
        val podA = consumer("pod-a", timeoutMillis = 600)
        val claimed = claimOne(podA)
        val heartbeat = launch { podA.runClaimHeartbeat() }

        podA.release(claimed) // processing threw
        delay(700)
        val redelivered = tryClaim(consumer("pod-b", timeoutMillis = 60_000), timeoutMs = 3_000)

        assertEquals(execution.id, redelivered?.execution?.id)
        heartbeat.cancel()
    }
}
