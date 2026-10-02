package run.trama.runtime

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import run.trama.config.ReconcilerConfig
import run.trama.saga.ExecutionState
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.telemetry.Metrics

/** Minimal repository surface needed by [ExecutionReconciler]. */
interface StalledExecutionRepository {
    /**
     * Claims running or sleeping executions that should have moved more than [staleAfterMillis]
     * ago, at a new checkpoint seq (any older copy still in the queue becomes stale).
     */
    suspend fun claimStalledExecutions(staleAfterMillis: Long, limit: Int): List<SagaExecution>
}

/**
 * Re-sends executions whose queue item no longer exists. Postgres holds every execution's
 * checkpoint, so an execution that stopped advancing although nothing is scheduled for it — its
 * Redis queue item was lost to a restart without persistence, a FLUSHALL or a failover, or its
 * worker died after a checkpoint and before handing the work on — is enqueued again from there.
 *
 * Every pod runs it; rows are claimed with SKIP LOCKED and their seq bumped, so each stalled
 * execution is re-sent once per stale period no matter how many pods scan.
 */
class ExecutionReconciler(
    private val repository: StalledExecutionRepository,
    private val enqueuer: SagaEnqueuer,
    private val metrics: Metrics,
    private val config: ReconcilerConfig,
) {
    private val logger = LoggerFactory.getLogger(ExecutionReconciler::class.java)

    suspend fun runLoop() {
        while (true) {
            delay(config.intervalMillis)
            if (!config.enabled) continue
            try {
                val requeued = scan()
                if (requeued > 0) logger.warn("reconciler re-sent stalled executions", kv("requeued", requeued))
            } catch (ex: CancellationException) {
                throw ex
            } catch (ex: Exception) {
                logger.warn("reconciler scan failed", ex)
            }
        }
    }

    /** Visible for testing. */
    suspend fun scan(): Int {
        val stalled = repository.claimStalledExecutions(config.staleAfterMillis, config.batchSize)
        for (execution in stalled) {
            enqueuer.enqueue(execution, 0)
            logger.info(
                "reconciler: re-enqueued stalled execution",
                kv("sagaId", execution.id.toString()),
                kv("state", execution.state::class.simpleName),
            )
        }
        stalled.groupingBy { if (it.state is ExecutionState.Sleeping) "SLEEPING" else "IN_PROGRESS" }
            .eachCount()
            .forEach { (status, count) -> metrics.recordReconciled(status, count) }
        return stalled.size
    }
}
