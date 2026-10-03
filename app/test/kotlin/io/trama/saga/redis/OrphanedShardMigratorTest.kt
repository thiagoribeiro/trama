package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.store.IntegrationDb

/** Lowering the shard count must not strand queued work in shards that no longer exist. */
class OrphanedShardMigratorTest {
    private lateinit var prefix: String
    private val redis get() = IntegrationDb.redis

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        prefix = "it-migrate-${UUID.randomUUID()}"
    }

    private fun execution() = SagaExecution(
        definition = SagaDefinition("migrate", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = "a"),
    )

    /** Executions that land in a shard >= [newCount] under [oldCount] shards. */
    private fun orphansOf(old: RedisShardKeyspace, newCount: Int, n: Int) =
        generateSequence { execution() }.filter { old.virtualShardFor(it.id) >= newCount }.take(n).toList()

    private suspend fun membersOf(key: String) = redis.withCommands { it.zrangeWithScores(key.encodeToByteArray()) }

    @Test
    fun `queued work moves to the shard it maps to under the new count, keeping its due time`() = runBlocking<Unit> {
        val old = RedisShardKeyspace(prefix, 1024)
        val new = RedisShardKeyspace(prefix, 64)
        val now = orphansOf(old, 64, 3)
        val later = orphansOf(old, 64, 1).single()
        now.forEach { RedisSagaEnqueuer(redis, old).enqueue(it, 0) }
        RedisSagaEnqueuer(redis, old).enqueue(later, 600_000)

        val moved = OrphanedShardMigrator(redis, new, RedisSagaEnqueuer(redis, new)).migrateOnce()

        assertEquals(4, moved)
        for (e in now + later) {
            assertTrue(membersOf(old.queueReadyKey(old.virtualShardFor(e.id))).isEmpty(), "old shard emptied")
            val there = membersOf(new.queueReadyKey(new.virtualShardFor(e.id)))
            assertEquals(1, there.count { MsgPack().decodeFromByteArray(SagaExecution.serializer(), it.first).id == e.id })
        }
        val laterScore = membersOf(new.queueReadyKey(new.virtualShardFor(later.id))).single { 
            MsgPack().decodeFromByteArray(SagaExecution.serializer(), it.first).id == later.id
        }.second
        assertTrue(laterScore > System.currentTimeMillis() + 500_000, "a delayed item stays delayed")
        assertEquals(0, OrphanedShardMigrator(redis, new, RedisSagaEnqueuer(redis, new)).migrateOnce(), "nothing left to move")
    }

    @Test
    fun `live claims in a removed shard are left to their owner until their lease expires`() = runBlocking<Unit> {
        val old = RedisShardKeyspace(prefix, 1024)
        val new = RedisShardKeyspace(prefix, 64)
        val (live, expired) = orphansOf(old, 64, 2)
        val msgPack = MsgPack()
        for ((e, deadline) in listOf(live to System.currentTimeMillis() + 60_000, expired to System.currentTimeMillis() - 1)) {
            val payload = msgPack.encodeToByteArray(SagaExecution.serializer(), e)
            redis.withCommands { it.zadd(old.queueInFlightKey(old.virtualShardFor(e.id)).encodeToByteArray(), deadline.toDouble(), payload) }
        }

        val moved = OrphanedShardMigrator(redis, new, RedisSagaEnqueuer(redis, new)).migrateOnce()

        assertEquals(1, moved)
        assertEquals(1, membersOf(old.queueInFlightKey(old.virtualShardFor(live.id))).size, "live claim untouched")
        assertTrue(membersOf(old.queueInFlightKey(old.virtualShardFor(expired.id))).isEmpty())
        assertEquals(1, membersOf(new.queueReadyKey(new.virtualShardFor(expired.id))).size, "expired claim re-queued")
    }
}
