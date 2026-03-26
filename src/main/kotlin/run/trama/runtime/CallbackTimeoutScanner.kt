package run.trama.runtime

import kotlinx.coroutines.delay
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import run.trama.config.CallbackTimeoutScannerConfig
import run.trama.saga.SagaEnqueuer
import run.trama.saga.WaitingInfo
import run.trama.telemetry.Metrics
import java.util.UUID

/**
 * Postgres fallback scanner that re-enqueues WAITING_CALLBACK executions whose
 * callback deadline has passed without the Redis sentinel delivering them.
 *
 * This is a defensive backstop for rare cases (e.g., Redis data loss, pod restart
 * during the wait window). The primary timeout path is handled in-line by
 * [run.trama.saga.workflow.WorkflowExecutor.executeWaitingCallback] when the Redis
 * ZSET sentinel score fires.
 *
 * A [bufferSeconds] grace period prevents double-processing items whose sentinel is
 * still in-flight inside the Redis ZSET.
 */
/** Minimal repository surface needed by [CallbackTimeoutScanner]. */
interface CallbackTimeoutRepository {
    suspend fun findExpiredWaitingExecutions(bufferSeconds: Long = 120, limit: Int = 100): List<UUID>
    suspend fun consumeWaitingState(executionId: UUID): WaitingInfo?
}

class CallbackTimeoutScanner(
    private val repository: CallbackTimeoutRepository,
    private val enqueuer: SagaEnqueuer,
    private val metrics: Metrics,
    private val config: CallbackTimeoutScannerConfig,
) {
    private val logger = LoggerFactory.getLogger(CallbackTimeoutScanner::class.java)

    suspend fun runLoop() {
        while (true) {
            delay(config.intervalMillis)
            if (!config.enabled) continue
            try {
                val requeued = scan()
                if (requeued > 0) {
                    logger.info("callback timeout fallback scan completed", kv("requeued", requeued))
                    metrics.recordCallbackTimeoutScan(requeued)
                }
            } catch (ex: Exception) {
                logger.warn("callback timeout fallback scan failed", ex)
            }
        }
    }

    /** Visible for testing. */
    suspend fun scan(): Int {
        val ids = repository.findExpiredWaitingExecutions(
            bufferSeconds = config.bufferSeconds,
            limit = config.batchSize,
        )
        if (ids.isEmpty()) return 0

        var requeued = 0
        for (executionId in ids) {
            try {
                val waiting = repository.consumeWaitingState(executionId) ?: continue
                val execution = waiting.execution
                // Re-enqueue with score = now so the consumer picks it up immediately.
                // WorkflowExecutor will see WaitingCallback state with an expired deadline
                // and trigger the normal timeout/retry/compensation path.
                enqueuer.enqueue(execution, 0)
                requeued++
                logger.info(
                    "callback timeout fallback: re-enqueued",
                    kv("executionId", executionId.toString()),
                    kv("nodeId", waiting.nodeId),
                )
            } catch (ex: Exception) {
                logger.warn(
                    "callback timeout fallback: failed to re-enqueue",
                    kv("executionId", executionId.toString()),
                    ex,
                )
            }
        }
        return requeued
    }
}
