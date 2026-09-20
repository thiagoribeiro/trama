package run.trama.runtime

import kotlinx.coroutines.delay
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import run.trama.config.JoinCompletionScannerConfig
import java.util.UUID

/**
 * Postgres fallback scanner that resumes parents whose join barrier is already satisfied
 * (arrived_count >= expected_count) but are still sitting in WAITING_JOIN — i.e. the eager
 * resume in [run.trama.saga.workflow.WorkflowExecutor.finalizeAndNotifyParent] ran the
 * increment but crashed (or lost its Redis fast path) before actually resuming the parent.
 */
interface JoinBarrierRepository {
    suspend fun findStalledJoinBarriers(limit: Int = 100): List<UUID>
}

interface JoinResumer {
    /** Returns true if a join was actually resumed (false if already handled by someone else). */
    suspend fun resumeJoinIfSatisfied(parentId: UUID): Boolean
}

class JoinCompletionScanner(
    private val repository: JoinBarrierRepository,
    private val resumer: JoinResumer,
    private val config: JoinCompletionScannerConfig,
) {
    private val logger = LoggerFactory.getLogger(JoinCompletionScanner::class.java)

    suspend fun runLoop() {
        while (true) {
            delay(config.intervalMillis)
            if (!config.enabled) continue
            try {
                val resumed = scan()
                if (resumed > 0) {
                    logger.info("join completion fallback scan completed", kv("resumed", resumed))
                }
            } catch (ex: Exception) {
                logger.warn("join completion fallback scan failed", ex)
            }
        }
    }

    /** Visible for testing. */
    suspend fun scan(): Int {
        val ids = repository.findStalledJoinBarriers(config.batchSize)
        var resumed = 0
        for (parentId in ids) {
            try {
                if (resumer.resumeJoinIfSatisfied(parentId)) {
                    resumed++
                    logger.info("join completion fallback: resumed stalled join", kv("parentId", parentId.toString()))
                }
            } catch (ex: Exception) {
                logger.warn("join completion fallback: failed to resume", kv("parentId", parentId.toString()), ex)
            }
        }
        return resumed
    }
}
