package run.trama.saga.redis

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

class RedisShardKeyspace(
    private val queueKeyPrefix: String,
    private val virtualShardCount: Int,
) {
    init {
        require(virtualShardCount > 0) { "virtualShardCount must be positive" }
    }

    fun virtualShardFor(executionId: UUID): Int {
        val buffer = ByteBuffer.allocate(16)
            .putLong(executionId.mostSignificantBits)
            .putLong(executionId.leastSignificantBits)
            .array()
        var hash = FNV64_OFFSET_BASIS
        for (byte in buffer) {
            hash = hash xor (byte.toLong() and 0xff)
            hash *= FNV64_PRIME
        }
        return floorMod(hash, virtualShardCount.toLong()).toInt()
    }

    fun queueReadyKey(shardId: Int): String =
        "$queueKeyPrefix:${queueTag(shardId)}:ready"

    fun queueInFlightKey(shardId: Int): String =
        "$queueKeyPrefix:${queueTag(shardId)}:inflight"

    fun executionMetaKey(executionId: UUID): String =
        "saga_executions:${queueTag(virtualShardFor(executionId))}:$executionId"

    fun executionStepsKey(executionId: UUID): String =
        "saga_executions:${queueTag(virtualShardFor(executionId))}:$executionId:steps"

    fun rateLimitCountKey(sagaName: String, keyPrefix: String): ByteArray =
        "$keyPrefix:{rl:$sagaName}:count".toByteArray(StandardCharsets.UTF_8)

    fun rateLimitBlockedKey(sagaName: String, keyPrefix: String): ByteArray =
        "$keyPrefix:{rl:$sagaName}:blocked".toByteArray(StandardCharsets.UTF_8)

    fun shardIds(): IntRange = 0 until virtualShardCount

    private fun queueTag(shardId: Int): String = "{vs-${shardId.toString().padStart(4, '0')}}"

    private fun floorMod(value: Long, divisor: Long): Long {
        val remainder = value % divisor
        return if (remainder < 0) remainder + divisor else remainder
    }

    private companion object {
        const val FNV64_OFFSET_BASIS = -3750763034362895579L
        const val FNV64_PRIME = 1099511628211L
    }
}
