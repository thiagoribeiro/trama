package run.trama.saga.redis

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.telemetry.Metrics
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RedisRuntimeIntegrationTest {
    @Test
    fun `stop polling leaves unclaimed work on ready queue`() = runBlocking<Unit> {
        run.trama.saga.store.IntegrationDb.assumeDocker()

        withRedisContainer { redis ->
            val keyspace = RedisShardKeyspace("it-runtime-${UUID.randomUUID()}", 64)
            val allocator = RendezvousShardAllocator(localPodId = "pod-a", virtualShardCount = 64)
            allocator.updatePods(listOf("pod-a"))
            val consumer = SagaExecutionRedisConsumer(
                redis = redis,
                keyspace = keyspace,
                allocator = allocator,
                batchSize = 1,
                processingTimeoutMillis = 60_000,
                claimerCount = 1,
                metrics = Metrics(SimpleMeterRegistry()),
            )
            val enqueuer = RedisSagaEnqueuer(redis, keyspace)
            val channel = Channel<ClaimedExecution>(capacity = 0)

            val first = testExecution()
            val shardId = keyspace.virtualShardFor(first.id)
            val second = testExecution(shardId = shardId, keyspace = keyspace)
            enqueuer.enqueue(first, 0)
            enqueuer.enqueue(second, 0)

            val producer = launch { consumer.runProducer(channel, emptyPollDelayMillis = 10) }

            awaitCondition {
                readyCount(redis, keyspace, shardId) == 1 &&
                    inFlightCount(redis, keyspace, shardId) == 1
            }

            consumer.stopPolling()
            val claimed = withTimeout(5_000) { channel.receive() }
            withTimeout(5_000) { producer.join() }
            consumer.ack(claimed)

            awaitCondition {
                readyCount(redis, keyspace, shardId) == 1 &&
                    inFlightCount(redis, keyspace, shardId) == 0
            }

            assertTrue(claimed.execution.id == first.id || claimed.execution.id == second.id)
        }
    }

    @Test
    fun `membership handoff reassigns shards and new owner can claim queued work`() = runBlocking<Unit> {
        run.trama.saga.store.IntegrationDb.assumeDocker()

        withRedisContainer { redis ->
            val virtualShardCount = 64
            val keyspace = RedisShardKeyspace("it-runtime-${UUID.randomUUID()}", virtualShardCount)
            val metrics = Metrics(SimpleMeterRegistry())
            val membershipKey = "it:pods:${UUID.randomUUID()}"
            val allocatorA = RendezvousShardAllocator(localPodId = "pod-a", virtualShardCount = virtualShardCount)
            val allocatorB = RendezvousShardAllocator(localPodId = "pod-b", virtualShardCount = virtualShardCount)
            val membershipA = PodMembershipRegistry(
                redis = redis,
                membershipKey = membershipKey,
                podId = "pod-a",
                membershipTtlMillis = 10_000,
                heartbeatIntervalMillis = 1_000,
                refreshIntervalMillis = 1_000,
                allocator = allocatorA,
                metrics = metrics,
            )
            val membershipB = PodMembershipRegistry(
                redis = redis,
                membershipKey = membershipKey,
                podId = "pod-b",
                membershipTtlMillis = 10_000,
                heartbeatIntervalMillis = 1_000,
                refreshIntervalMillis = 1_000,
                allocator = allocatorB,
                metrics = metrics,
            )

            membershipA.initialize()
            membershipB.initialize()
            membershipA.initialize()
            membershipB.initialize()

            val ownedByA = allocatorA.ownedShards().toSet()
            val ownedByB = allocatorB.ownedShards().toSet()
            assertTrue(ownedByA.isNotEmpty())
            assertTrue(ownedByA.intersect(ownedByB).isEmpty())
            assertEquals(virtualShardCount, ownedByA.size + ownedByB.size)

            val targetShard = ownedByA.first()
            val execution = testExecution(shardId = targetShard, keyspace = keyspace)
            RedisSagaEnqueuer(redis, keyspace).enqueue(execution, 0)

            membershipA.unregister()
            membershipB.initialize()

            assertTrue(targetShard in allocatorB.ownedShards())
            assertEquals(virtualShardCount, allocatorB.ownedShards().size)

            val consumerB = SagaExecutionRedisConsumer(
                redis = redis,
                keyspace = keyspace,
                allocator = allocatorB,
                batchSize = 1,
                processingTimeoutMillis = 60_000,
                claimerCount = 1,
                metrics = metrics,
            )
            val channel = Channel<ClaimedExecution>(1)
            val producer = launch { consumerB.runProducer(channel, emptyPollDelayMillis = 10) }

            val claimed = withTimeout(5_000) { channel.receive() }
            consumerB.stopPolling()
            consumerB.ack(claimed)
            withTimeout(5_000) { producer.join() }

            assertEquals(execution.id, claimed.execution.id)
        }
    }

    // Shared container (a dedicated one per test intermittently failed to launch under load);
    // every key below uses a per-test prefix so nothing collides with other suites.
    private suspend fun withRedisContainer(block: suspend (RedisClientProvider) -> Unit) {
        block(run.trama.saga.store.IntegrationDb.redis)
    }

    private suspend fun readyCount(
        redis: RedisClientProvider,
        keyspace: RedisShardKeyspace,
        shardId: Int,
    ): Int = redis.withCommands { commands ->
        commands.zrangebyscore(
            keyspace.queueReadyKey(shardId).encodeToByteArray(),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
        ).size
    }

    private suspend fun inFlightCount(
        redis: RedisClientProvider,
        keyspace: RedisShardKeyspace,
        shardId: Int,
    ): Int = redis.withCommands { commands ->
        commands.zrangebyscore(
            keyspace.queueInFlightKey(shardId).encodeToByteArray(),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
        ).size
    }

    private suspend fun awaitCondition(
        timeoutMillis: Long = 5_000,
        condition: suspend () -> Boolean,
    ) {
        withTimeout(timeoutMillis) {
            while (!condition()) {
                delay(25)
            }
        }
    }

    private fun testExecution(
        shardId: Int? = null,
        keyspace: RedisShardKeyspace? = null,
    ): SagaExecution {
        val executionId = when {
            shardId == null || keyspace == null -> UUID.randomUUID()
            else -> uuidForShard(keyspace, shardId)
        }
        return SagaExecution(
            definition = SagaDefinition(
                name = "order",
                version = "v1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = emptyList(),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = executionId,
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = emptyMap(),
        )
    }

    private fun uuidForShard(
        keyspace: RedisShardKeyspace,
        shardId: Int,
    ): UUID {
        repeat(20_000) {
            val candidate = UUID.randomUUID()
            if (keyspace.virtualShardFor(candidate) == shardId) {
                return candidate
            }
        }
        error("Could not find UUID for shard $shardId")
    }
}
