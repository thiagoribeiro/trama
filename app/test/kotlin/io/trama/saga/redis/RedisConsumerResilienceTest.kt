package run.trama.saga.redis

import io.lettuce.core.RedisClient
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import run.trama.e2e.E2EContainers
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb
import run.trama.telemetry.Metrics

/** The claim loop under Redis trouble, the due index and claim capacity. */
class RedisConsumerResilienceTest {
    private lateinit var keyspace: RedisShardKeyspace

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        keyspace = RedisShardKeyspace("it-resilience-${UUID.randomUUID()}", 4)
    }

    private fun consumer(
        redis: RedisCommandsProvider = IntegrationDb.redis,
        timeoutMillis: Long = 60_000,
        fullSweepIntervalMillis: Long = 5_000,
        batchSize: Int = 10,
    ) = SagaExecutionRedisConsumer(
        redis = redis,
        keyspace = keyspace,
        allocator = RendezvousShardAllocator("pod-a", 4).also { it.updatePods(listOf("pod-a")) },
        batchSize = batchSize,
        processingTimeoutMillis = timeoutMillis,
        claimerCount = 1,
        metrics = Metrics(SimpleMeterRegistry()),
        fullSweepIntervalMillis = fullSweepIntervalMillis,
    )

    private fun execution() = SagaExecution(
        definition = SagaDefinition("resilience", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = "a"),
    )

    private suspend fun enqueue(execution: SagaExecution = execution(), delayMillis: Long = 0): SagaExecution {
        RedisSagaEnqueuer(IntegrationDb.redis, keyspace).enqueue(execution, delayMillis)
        return execution
    }

    /** Runs [pod]'s claim loop until it has claimed [count] items or [timeoutMs] passes. */
    private suspend fun claim(
        pod: SagaExecutionRedisConsumer,
        count: Int = 1,
        timeoutMs: Long = 5_000,
        permits: ClaimPermits = ClaimPermits.UNBOUNDED,
        afterStart: suspend () -> Unit = {},
    ): List<ClaimedExecution> = coroutineScope {
        val channel = Channel<ClaimedExecution>(100)
        val producer = launch { pod.runProducer(channel, emptyPollDelayMillis = 10, permits = permits) }
        afterStart()
        val claimed = mutableListOf<ClaimedExecution>()
        withTimeoutOrNull(timeoutMs) { repeat(count) { claimed += channel.receive() } }
        pod.stopPolling()
        producer.join()
        generateSequence { channel.tryReceive().getOrNull() }.forEach { claimed += it }
        claimed
    }

    @Test
    fun `claims keep working after Redis forgets its scripts`() = runBlocking<Unit> {
        val pod = consumer()
        val first = enqueue()
        assertEquals(first.id, claim(pod).single().execution.id)

        val r = E2EContainers.redis
        RedisClient.create("redis://${r.host}:${r.getMappedPort(6379)}").use { client ->
            client.connect().use { it.sync().scriptFlush() } // what a Redis restart does to the cache
        }
        val second = enqueue()

        assertEquals(second.id, claim(consumer()).single().execution.id)
    }

    @Test
    fun `a claimer survives Redis errors and resumes consuming`() = runBlocking<Unit> {
        val failuresLeft = AtomicInteger(5)
        val flaky = object : RedisCommandsProvider {
            override suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T {
                if (failuresLeft.getAndDecrement() > 0) throw io.lettuce.core.RedisConnectionException("connection refused")
                return IntegrationDb.redis.withCommands(block)
            }
        }
        val execution = enqueue()

        val claimed = claim(consumer(redis = flaky), timeoutMs = 15_000)

        assertEquals(execution.id, claimed.single().execution.id)
    }

    @Test
    fun `an item enqueued with a delay is claimed when due, without waiting for a full sweep`() = runBlocking<Unit> {
        val pod = consumer(fullSweepIntervalMillis = 600_000)
        lateinit var execution: SagaExecution
        val started = System.currentTimeMillis()

        // The first pass is a full sweep over empty shards; the item only appears afterwards.
        val claimed = claim(pod, timeoutMs = 5_000) {
            delay(200)
            execution = enqueue(delayMillis = 500)
        }

        assertEquals(execution.id, claimed.single().execution.id)
        assert(System.currentTimeMillis() - started >= 700) { "claimed before it was due" }
    }

    @Test
    fun `a process never claims beyond its free capacity`() = runBlocking<Unit> {
        repeat(5) { enqueue() }

        val claimed = claim(consumer(), count = 5, timeoutMs = 1_500, permits = ClaimPermits(2))

        assertEquals(2, claimed.size, "only as many claims as there are permits")
    }

    @Test
    fun `the heartbeat fences a claim whose lease ran out`() = runBlocking<Unit> {
        enqueue()
        val podA = consumer(timeoutMillis = 300)
        val claimed = claim(podA).single()
        val lease = assertNotNull(claimed.lease)

        delay(400) // paused past the lease; another pod may recover and claim the item now
        claim(consumer()).single()
        val heartbeat = launch { podA.runClaimHeartbeat() }
        delay(250)
        heartbeat.cancel()

        assertFalse(lease.isHeld())
    }

    @Test
    fun `a live claim erased by a Redis data loss is restored, not fenced`() = runBlocking<Unit> {
        enqueue()
        val pod = consumer(timeoutMillis = 900)
        val claimed = claim(pod).single()
        val inflight = keyspace.queueInFlightKey(claimed.shardId).encodeToByteArray()
        IntegrationDb.redis.withCommands { it.del(inflight) } // FLUSHALL, failover without persistence...

        val heartbeat = launch { pod.runClaimHeartbeat() }
        delay(700)
        heartbeat.cancel()

        kotlin.test.assertTrue(assertNotNull(claimed.lease).isHeld(), "the worker keeps its claim")
        val restored = IntegrationDb.redis.withCommands { it.zrangebyscore(inflight, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY) }
        assertEquals(1, restored.size, "and it is back in flight, so it is not handed to anyone else")
    }
}
