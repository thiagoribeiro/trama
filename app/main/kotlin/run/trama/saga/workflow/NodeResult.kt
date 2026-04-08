package run.trama.saga.workflow

import run.trama.saga.FailureReason
import java.time.Instant

sealed class NodeResult {
    /** Node completed successfully; advance to [nextNodeId] (null = terminal). */
    data class Advanced(val nextNodeId: String?) : NodeResult()

    /** Node failed; [retryable] indicates whether a retry attempt is applicable. */
    data class NodeFailed(
        val retryable: Boolean,
        val reason: FailureReason,
        val statusCode: Int? = null,
        val responseBody: String? = null,
    ) : NodeResult()

    /** Async node accepted; waiting for external callback before advancing. */
    data class WaitingForCallback(
        val nodeId: String,
        val attempt: Int,
        val deadlineAt: Instant,
        val nonce: String,
        val signature: String,
    ) : NodeResult()
}
