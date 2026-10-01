package run.trama.saga.redis

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb
import run.trama.saga.store.SagaRepository
import run.trama.telemetry.Metrics

class RedisSleepAndMembershipTest {
    private lateinit var repo: SagaRepository
    private lateinit var store: RedisSagaExecutionStore

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        repo = SagaRepository(IntegrationDb.client)
        store = RedisSagaExecutionStore(IntegrationDb.redis, repo, RedisShardKeyspace("it-${UUID.randomUUID()}", 64))
    }

    private fun sleepingExecution(wakeAt: Instant = Instant.now().plusSeconds(60)) = SagaExecution(
        definition = SagaDefinition("sleep-it-${UUID.randomUUID()}", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.Sleeping(wakeAt, nextNodeId = "after", completedNodes = listOf("before"), compensationStack = emptyList()),
    )

    // ── Sleep sentinel ──────────────────────────────────────────────────────

    @Test
    fun `sleep entry can be peeked repeatedly but consumed exactly once`() = runBlocking<Unit> {
        val exec = sleepingExecution()
        repo.upsertExecutionStart(exec)
        val wakeAt = (exec.state as ExecutionState.Sleeping).wakeAt

        store.saveSleeping(exec, wakeAt)

        assertEquals("SLEEPING", repo.getExecutionStatus(exec.id)?.status, "status API must surface SLEEPING")
        assertNotNull(store.peekSleeping(exec.id))
        val peeked = assertNotNull(store.peekSleeping(exec.id), "peek must not consume")
        assertEquals(wakeAt.epochSecond, peeked.wakeAt.epochSecond)
        val state = assertIs<ExecutionState.Sleeping>(peeked.execution.state)
        assertEquals("after", state.nextNodeId)
        assertEquals(listOf("before"), state.completedNodes)

        assertNotNull(store.consumeSleeping(exec.id))
        assertNull(store.consumeSleeping(exec.id))
        assertNull(store.peekSleeping(exec.id))
    }

    @Test
    fun `concurrent wake attempts never both win`() = runBlocking<Unit> {
        val exec = sleepingExecution()
        repo.upsertExecutionStart(exec)
        store.saveSleeping(exec, Instant.now().plusSeconds(60))

        // Wake endpoint racing the worker that dequeues the item right at wakeAt.
        val results = (1..8).map { async { store.consumeSleeping(exec.id) } }.awaitAll()

        assertEquals(1, results.count { it != null }, "exactly one consumer must win: $results")
    }

    @Test
    fun `saveSleeping ignores executions that are not in Sleeping state`() = runBlocking<Unit> {
        val exec = sleepingExecution().copy(state = ExecutionState.InProgress(activeNodeId = "x"))
        store.saveSleeping(exec, Instant.now().plusSeconds(60))
        assertNull(store.peekSleeping(exec.id))
    }

    // ── Pod membership ──────────────────────────────────────────────────────

    private fun registry(key: String, podId: String, ttlMillis: Long = 10_000, allocator: RendezvousShardAllocator = RendezvousShardAllocator(podId, 64)) =
        PodMembershipRegistry(
            redis = IntegrationDb.redis,
            membershipKey = key,
            podId = podId,
            membershipTtlMillis = ttlMillis,
            heartbeatIntervalMillis = 100,
            refreshIntervalMillis = 100,
            allocator = allocator,
            metrics = Metrics(SimpleMeterRegistry()),
        )

    @Test
    fun `readiness reflects membership lifecycle`() = runBlocking<Unit> {
        val key = "it:pods:${UUID.randomUUID()}"
        val pod = registry(key, "pod-a")

        assertEquals("membership_not_initialized", pod.readiness().message)
        pod.initialize()
        assertTrue(pod.readiness().ok, pod.readiness().message)

        pod.unregister()
        assertEquals("pod_not_registered", pod.readiness().message)
    }

    @Test
    fun `pods see each other and split all shards between them`() = runBlocking<Unit> {
        val key = "it:pods:${UUID.randomUUID()}"
        val allocA = RendezvousShardAllocator("pod-a", 64)
        val allocB = RendezvousShardAllocator("pod-b", 64)
        val a = registry(key, "pod-a", allocator = allocA)
        val b = registry(key, "pod-b", allocator = allocB)
        a.initialize(); b.initialize(); a.initialize()

        assertEquals(listOf("pod-a", "pod-b"), allocA.activePods())
        assertEquals(64, allocA.ownedShards().size + allocB.ownedShards().size)
        assertTrue(allocA.ownedShards().intersect(allocB.ownedShards().toSet()).isEmpty())
    }

    @Test
    fun `a pod that stops heartbeating expires after the TTL and its shards are taken over`() = runBlocking<Unit> {
        val key = "it:pods:${UUID.randomUUID()}"
        val allocA = RendezvousShardAllocator("pod-a", 64)
        val a = registry(key, "pod-a", ttlMillis = 60_000, allocator = allocA)
        val dying = registry(key, "pod-dying", ttlMillis = 300)
        dying.initialize()
        a.initialize()
        assertEquals(listOf("pod-a", "pod-dying"), allocA.activePods())
        assertTrue(allocA.ownedShards().size < 64)

        delay(600) // pod-dying never heartbeats again (simulated crash, no unregister)
        a.initialize()

        assertEquals(listOf("pod-a"), allocA.activePods())
        assertEquals(64, allocA.ownedShards().size)
    }

    @Test
    fun `readiness goes stale when refresh stops for longer than the TTL`() = runBlocking<Unit> {
        val pod = registry("it:pods:${UUID.randomUUID()}", "pod-a", ttlMillis = 200)
        pod.initialize()
        assertTrue(pod.readiness().ok)
        delay(350)
        assertFalse(pod.readiness().ok)
        assertEquals("membership_stale", pod.readiness().message)
    }
}
