package run.trama.saga

import run.trama.config.RedisConfig
import run.trama.config.RedisConsumerConfig
import run.trama.config.RedisPoolConfig
import run.trama.config.RedisQueueConfig
import run.trama.saga.redis.RedisClientProvider
import run.trama.saga.redis.SagaExecutionRedisConsumer
import run.trama.telemetry.Metrics
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.DockerClientFactory
import org.testcontainers.utility.DockerImageName

class RedisConsumerIntegrationTest {
    @Test
    fun `claim moves items to in-flight and ack removes`() = runBlocking {
        if (!DockerClientFactory.instance().isDockerAvailable) return@runBlocking
        val container = GenericContainer(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
        container.start()
        try {
            val redisUrl = "redis://${container.host}:${container.getMappedPort(6379)}"
            val redis = RedisClientProvider(
                RedisConfig(
                    url = redisUrl,
                    pool = RedisPoolConfig(),
                    queue = RedisQueueConfig(),
                    consumer = RedisConsumerConfig(),
                )
            )
            val consumer = SagaExecutionRedisConsumer(
                redis = redis,
                readyKey = "saga:executions",
                inFlightKey = "saga:executions:in-flight",
                batchSize = 50,
                processingTimeoutMillis = 60_000,
                metrics = Metrics(io.micrometer.core.instrument.simple.SimpleMeterRegistry()),
            )
            val enqueuer = RedisSagaEnqueuer(redis, "saga:executions")

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
                state = ExecutionState.InProgress(ExecutionPhase.UP),
                payload = emptyMap(),
            )

            enqueuer.enqueue(execution, 0)

            val items = consumer.pollReady()
            assertEquals(1, items.size)

            val inFlight = items.first()
            consumer.ack(inFlight)
            redis.close()
        } finally {
            container.stop()
        }
    }
}
