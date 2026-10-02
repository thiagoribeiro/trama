package run.trama.saga

import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withContext
import run.trama.saga.redis.ClaimPermits
import run.trama.saga.redis.ClaimedExecution
import run.trama.saga.redis.SagaRateLimiter
import run.trama.saga.redis.SagaExecutionConsumer
import run.trama.telemetry.Metrics
import run.trama.telemetry.Tracing
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv

interface SagaExecutor {
    suspend fun execute(execution: SagaExecution): ExecutionOutcome
}

sealed class ExecutionOutcome {
    data object Succeeded : ExecutionOutcome()
    data object FailedFinal : ExecutionOutcome()
    data object Reenqueued : ExecutionOutcome()
}

class SagaExecutionProcessor(
    private val consumer: SagaExecutionConsumer,
    private val executor: SagaExecutor,
    private val enqueuer: SagaEnqueuer,
    private val rateLimiter: SagaRateLimiter,
    private val metrics: Metrics,
    /**
     * Most claimed executions this process holds at once (running + waiting for a worker):
     * worker count plus a small prefetch. Claiming beyond it only parks work where no other pod
     * can reach it.
     */
    claimCapacity: Int = 8,
    private val emptyPollDelayMillis: Long = 50,
) {
    private val capacity = claimCapacity.coerceAtLeast(1)
    private val permits = ClaimPermits(capacity)
    private val running = java.util.concurrent.atomic.AtomicInteger(0)
    private val buffer = Channel<ClaimedExecution>(claimCapacity.coerceAtLeast(1))
    private val logger = LoggerFactory.getLogger(SagaExecutionProcessor::class.java)
    private val tracer = Tracing.tracer("saga-processor")

    suspend fun runProducer() {
        try {
            consumer.runProducer(buffer, emptyPollDelayMillis, permits)
        } finally {
            buffer.close()
        }
    }

    fun stopPolling() {
        consumer.stopPolling()
    }

    suspend fun runWorker() {
        for (item in buffer) {
            running.incrementAndGet()
            reportWaiting()
            try {
                process(item)
            } finally {
                running.decrementAndGet()
                permits.release()
                reportWaiting()
            }
        }
    }

    /** saga_inmemory_queue_size: claimed by this process and waiting for a free worker. */
    private fun reportWaiting() {
        metrics.setQueueSize((capacity - permits.available() - running.get()).coerceAtLeast(0).toLong())
    }

    private suspend fun process(item: ClaimedExecution) {
        val execution = item.execution
        try {
            val delayMillis = rateLimiter.checkDelayMillis(execution.definition.name)
            if (delayMillis != null && delayMillis > 0) {
                logger.info(
                    "rate limited saga",
                    kv("sagaId", execution.id.toString()),
                    kv("sagaName", execution.definition.name),
                    kv("delayMillis", delayMillis),
                )
                metrics.recordRateLimited(execution)
                enqueuer.enqueue(execution, delayMillis)
                consumer.ack(item)
                return
            }
            val outcome = withContext(item.lease ?: EmptyCoroutineContext) {
                Tracing.withSpan(
                    tracer = tracer,
                    name = "saga.process",
                    attributes = mapOf(
                        "saga.id" to execution.id.toString(),
                        "saga.name" to execution.definition.name,
                    )
                ) { span ->
                    Tracing.withTraceMdc(span, execution.id.toString()) {
                        logger.info(
                            "processing saga",
                            kv("sagaName", execution.definition.name),
                        )
                    }
                    executor.execute(execution)
                }
            }
            when (outcome) {
                ExecutionOutcome.Succeeded,
                ExecutionOutcome.FailedFinal,
                ExecutionOutcome.Reenqueued,
                -> consumer.ack(item)
            }
            metrics.recordProcessed(execution, outcome.toMetricOutcome())
            // A FAILED outcome is the workflow's own business result, not a sign the runtime is
            // unhealthy: it is not fed to the rate limiter (it used to pause every execution of
            // the workflow after a handful of expected failures).
            if (outcome == ExecutionOutcome.FailedFinal) {
                metrics.recordFailed(execution, "failed_final")
            }
            if (outcome == ExecutionOutcome.Reenqueued) {
                metrics.recordRetried(execution)
            }
        } catch (ex: LeaseLostException) {
            // Paused past the claim deadline: the item may already be another worker's, so it
            // is neither acked nor failed here. Whoever holds it now carries on.
            logger.warn("claim lease lost; dropping work", kv("sagaId", execution.id.toString()))
            metrics.recordFenced("lease_lost")
            consumer.release(item)
        } catch (ex: StaleCheckpointException) {
            // The execution was advanced (or finished) from another copy of this item. This claim
            // is still ours, so it is acked: this copy has nothing left to do.
            logger.info("stale checkpoint; dropping duplicate work", kv("sagaId", execution.id.toString()))
            metrics.recordFenced("stale_checkpoint")
            runCatching { consumer.ack(item) }
        } catch (ex: Exception) {
            // This catch is mutually exclusive with the outcome block above:
            // executor.execute() either returns normally (handled above) or throws (handled here).
            logger.warn(
                "processing failed",
                kv("sagaId", execution.id.toString()),
                kv("sagaName", execution.definition.name),
                ex
            )
            try {
                metrics.recordFailed(execution, "worker_exception")
                rateLimiter.recordFailure(execution.definition.name)
            } catch (_: Exception) {
                // ignore
            }
            // Leave it in flight: stop renewing the claim so it expires and is re-delivered.
            consumer.release(item)
        }
    }

    private fun ExecutionOutcome.toMetricOutcome(): String =
        when (this) {
            ExecutionOutcome.Succeeded -> "SUCCEEDED"
            ExecutionOutcome.FailedFinal -> "FAILED_FINAL"
            ExecutionOutcome.Reenqueued -> "REENQUEUED"
        }
}
