package run.trama.saga

import run.trama.saga.redis.RedisShardKeyspace
import run.trama.saga.redis.RendezvousShardAllocator
import run.trama.saga.redis.SagaExecutionRedisConsumer
import run.trama.telemetry.Metrics
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class RedisConsumerIntegrationTest {
    @Test
    fun `claim moves items to in-flight and ack removes`() = runBlocking<Unit> {
        // Shared container (a dedicated one per test intermittently failed to launch under load).
        run.trama.saga.store.IntegrationDb.assumeDocker()
        run {
            val redis = run.trama.saga.store.IntegrationDb.redis
            val keyspace = RedisShardKeyspace("it-consumer-${java.util.UUID.randomUUID()}", 1024)
            val allocator = RendezvousShardAllocator(
                localPodId = "pod-a",
                virtualShardCount = 1024,
            )
            allocator.updatePods(listOf("pod-a"))
            val consumer = SagaExecutionRedisConsumer(
                redis = redis,
                keyspace = keyspace,
                allocator = allocator,
                batchSize = 50,
                processingTimeoutMillis = 60_000,
                claimerCount = 1,
                metrics = Metrics(io.micrometer.core.instrument.simple.SimpleMeterRegistry()),
            )
            val enqueuer = RedisSagaEnqueuer(redis, keyspace)

            val execution = SagaExecution(
                definition = SagaDefinition(
                    name = "test",
                    version = "1",
                    failureHandling = FailureHandling.Retry(1, 10),
                    steps = emptyList(),
                    onSuccessCallback = null,
                    onFailureCallback = null,
                ),
                id = java.util.UUID.randomUUID(),
                startedAt = java.time.Instant.now(),
                currentStepIndex = 0,
                state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
                payload = emptyMap(),
            )

            enqueuer.enqueue(execution, 0)

            val channel = Channel<run.trama.saga.redis.ClaimedExecution>(1)
            val producer = launch { consumer.runProducer(channel, emptyPollDelayMillis = 10) }
            val items = listOf(withTimeout(5_000) { channel.receive() })
            assertEquals(1, items.size)

            val inFlight = items.first()
            consumer.ack(inFlight)
            producer.cancel()
        }
    }
}
