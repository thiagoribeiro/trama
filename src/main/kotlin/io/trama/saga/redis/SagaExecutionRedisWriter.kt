@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionQueue
import run.trama.telemetry.Metrics
import kotlinx.serialization.encodeToByteArray
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

class SagaExecutionRedisWriter(
    private val queue: SagaExecutionQueue,
    private val redis: RedisCommandsProvider,
    private val redisKey: String,
    private val batchSize: Int = 20,
    private val maxWaitMillis: Long = 10,
    private val metrics: Metrics,
) {
    private val msgPack = MsgPack()

    private val redisKeyBytes: ByteArray = redisKey.encodeToByteArray()

    suspend fun run() {
        while (true) {
            val batch = receiveBatch()
            if (batch.isEmpty()) {
                kotlinx.coroutines.delay(5.milliseconds)
                continue
            }
            redis.withCommands { commands ->
                for (item in batch) {
                    val payload = msgPack.encodeToByteArray(SagaExecution.serializer(), item.execution)
                    commands.zadd(redisKeyBytes, item.receivedAtMillis.toDouble(), payload)
                }
            }
            batch.forEach { item ->
                metrics.recordEnqueued(item.execution)
            }
            metrics.setQueueSize(queue.size().toLong())
        }
    }

    private suspend fun receiveBatch(): List<QueuedExecution> {
        val first = queue.receive()
        val batch = mutableListOf(
            QueuedExecution(first, System.currentTimeMillis())
        )

        val started = TimeSource.Monotonic.markNow()
        while (batch.size < batchSize) {
            val elapsed = started.elapsedNow().inWholeMilliseconds
            val remaining = maxWaitMillis - elapsed
            if (remaining <= 0) {
                break
            }

            val immediate = queue.tryReceive()
            if (immediate != null) {
                batch.add(QueuedExecution(immediate, System.currentTimeMillis()))
                continue
            }

            val waited = kotlinx.coroutines.withTimeoutOrNull(remaining.milliseconds) {
                queue.receive()
            }
            if (waited == null) {
                break
            }
            batch.add(QueuedExecution(waited, System.currentTimeMillis()))
        }

        return batch
    }

    private data class QueuedExecution(
        val execution: SagaExecution,
        val receivedAtMillis: Long,
    )
}
