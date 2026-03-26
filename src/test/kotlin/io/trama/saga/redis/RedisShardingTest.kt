package run.trama.saga.redis

import io.lettuce.core.ScriptOutputType
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution

class RedisShardingTest {
    @Test
    fun `rendezvous ownership is deterministic`() {
        val left = RendezvousShardAllocator(localPodId = "pod-a", virtualShardCount = 1024)
        val right = RendezvousShardAllocator(localPodId = "pod-a", virtualShardCount = 1024)

        left.updatePods(listOf("pod-c", "pod-a", "pod-b"))
        right.updatePods(listOf("pod-b", "pod-a", "pod-c"))

        assertEquals(left.ownedShards(), right.ownedShards())
    }

    @Test
    fun `adding a pod only moves a subset of shards`() {
        val allocator = RendezvousShardAllocator(localPodId = "pod-a", virtualShardCount = 1024)
        val beforePods = listOf("pod-a", "pod-b")
        val afterPods = listOf("pod-a", "pod-b", "pod-c")

        val moved = (0 until 1024).count { shardId ->
            allocator.ownerFor(shardId, beforePods) != allocator.ownerFor(shardId, afterPods)
        }

        assertTrue(moved in 1 until 1024)
    }

    @Test
    fun `keyspace keeps related keys on the same cluster slot`() {
        val keyspace = RedisShardKeyspace("saga:executions", 1024)
        val executionId = UUID.fromString("2f9b5944-fca6-4900-bcd9-355cb68da3b0")
        val shardId = keyspace.virtualShardFor(executionId)

        val queueReady = keyspace.queueReadyKey(shardId)
        val queueInFlight = keyspace.queueInFlightKey(shardId)
        val metaKey = keyspace.executionMetaKey(executionId)
        val stepsKey = keyspace.executionStepsKey(executionId)

        assertEquals(redisClusterSlot(queueReady), redisClusterSlot(queueInFlight))
        assertEquals(redisClusterSlot(metaKey), redisClusterSlot(stepsKey))
        assertEquals(redisClusterSlot(queueReady), redisClusterSlot(metaKey))
    }

    @Test
    fun `enqueuer writes to the execution shard ready key`() = runBlocking {
        val keyspace = RedisShardKeyspace("saga:executions", 1024)
        var capturedKey: ByteArray? = null
        val provider = object : RedisCommandsProvider {
            override suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T {
                return block(object : RedisBinaryCommands {
                    override suspend fun zadd(key: ByteArray, score: Double, member: ByteArray): Long? {
                        capturedKey = key
                        return 1L
                    }

                    override suspend fun zrem(key: ByteArray, member: ByteArray): Long? = 1L
                    override suspend fun zremrangebyscore(key: ByteArray, min: Double, max: Double): Long? = 0L
                    override suspend fun zrangebyscore(
                        key: ByteArray,
                        min: Double,
                        max: Double,
                        limit: Int?,
                    ): List<ByteArray> = emptyList()

                    override suspend fun <T> eval(
                        script: ByteArray,
                        outputType: ScriptOutputType,
                        keys: Array<ByteArray>,
                        vararg values: ByteArray,
                    ): T? = null
                    override suspend fun scriptLoad(script: ByteArray): String = ""
                    override suspend fun <T> evalsha(digest: String, outputType: ScriptOutputType, keys: Array<ByteArray>, vararg values: ByteArray): T? = null

                    override suspend fun get(key: ByteArray): ByteArray? = null
                    override suspend fun incr(key: ByteArray): Long? = 0L
                    override suspend fun pexpire(key: ByteArray, milliseconds: Long): Boolean? = true
                    override suspend fun psetex(key: ByteArray, milliseconds: Long, value: ByteArray): String? = "OK"
                    override suspend fun expire(key: ByteArray, seconds: Long): Boolean? = true
                    override suspend fun set(key: ByteArray, value: ByteArray): String? = "OK"
                    override suspend fun setNx(key: ByteArray, value: ByteArray, ttlSeconds: Long): Boolean = true
                    override suspend fun del(vararg keys: ByteArray): Long? = 0L
                    override suspend fun lpush(key: ByteArray, value: ByteArray): Long? = 0L
                    override suspend fun lrange(key: ByteArray, start: Long, stop: Long): List<ByteArray> = emptyList()
                    override suspend fun ping(): String = "PONG"
                })
            }
        }
        val enqueuer = RedisSagaEnqueuer(provider, keyspace)
        val execution = testExecution()

        enqueuer.enqueue(execution, 0)

        assertEquals(
            keyspace.queueReadyKey(keyspace.virtualShardFor(execution.id)),
            capturedKey?.toString(StandardCharsets.UTF_8),
        )
    }

    private fun testExecution(): SagaExecution =
        SagaExecution(
            definition = SagaDefinition(
                name = "order",
                version = "v1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = emptyList(),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = UUID.fromString("4f70fd63-b3dc-4c85-8902-7506c27fbf19"),
            startedAt = Instant.parse("2026-03-14T00:00:00Z"),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = emptyMap(),
        )

    private fun redisClusterSlot(key: String): Int {
        val hashtag = extractHashTag(key).toByteArray(StandardCharsets.UTF_8)
        return crc16(hashtag) % 16384
    }

    private fun extractHashTag(key: String): String {
        val start = key.indexOf('{')
        val end = key.indexOf('}', start + 1)
        return if (start >= 0 && end > start + 1) {
            key.substring(start + 1, end)
        } else {
            key
        }
    }

    private fun crc16(bytes: ByteArray): Int {
        var crc = 0
        for (byte in bytes) {
            crc = crc xor ((byte.toInt() and 0xff) shl 8)
            repeat(8) {
                crc = if ((crc and 0x8000) != 0) {
                    (crc shl 1) xor 0x1021
                } else {
                    crc shl 1
                }
            }
            crc = crc and 0xffff
        }
        return crc
    }
}
