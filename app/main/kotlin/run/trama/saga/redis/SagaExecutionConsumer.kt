package run.trama.saga.redis

import kotlinx.coroutines.channels.SendChannel
import run.trama.saga.SagaExecution

interface SagaExecutionConsumer {
    suspend fun runProducer(
        buffer: SendChannel<ClaimedExecution>,
        emptyPollDelayMillis: Long,
    )

    suspend fun ack(inFlight: ClaimedExecution)

    fun stopPolling() {}
}

data class ClaimedExecution(
    val execution: SagaExecution,
    val payload: ByteArray,
    val shardId: Int,
)
