package run.trama.saga.redis

import kotlinx.coroutines.channels.SendChannel
import run.trama.saga.ClaimLease
import run.trama.saga.SagaExecution

interface SagaExecutionConsumer {
    /**
     * Claims work into [buffer] until polling stops. A claimer takes [permits] before claiming, so
     * the process never holds more claimed items than [permits] allows; the processor gives one
     * back per finished item.
     */
    suspend fun runProducer(
        buffer: SendChannel<ClaimedExecution>,
        emptyPollDelayMillis: Long,
        permits: ClaimPermits = ClaimPermits.UNBOUNDED,
    )

    suspend fun ack(inFlight: ClaimedExecution)

    /**
     * Gives up a claim without acking it: it stays in flight and is re-delivered once its deadline
     * passes (used when processing threw). Stops any renewal of that claim.
     */
    fun release(inFlight: ClaimedExecution) {}

    fun stopPolling() {}
}

data class ClaimedExecution(
    val execution: SagaExecution,
    val payload: ByteArray,
    val shardId: Int,
    /** Consumer-local handle for the claim (ack/release/renewal bookkeeping). */
    val claimId: Long = 0,
    /** This worker's hold on the claim; null when the consumer doesn't fence (tests). */
    val lease: ClaimLease? = null,
)
