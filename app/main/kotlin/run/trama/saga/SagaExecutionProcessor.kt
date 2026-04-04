package run.trama.saga

import kotlinx.coroutines.channels.Channel
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
    bufferSize: Int = 200,
    private val emptyPollDelayMillis: Long = 50,
) {
    private val buffer = Channel<ClaimedExecution>(bufferSize)
    private val logger = LoggerFactory.getLogger(SagaExecutionProcessor::class.java)
    private val tracer = Tracing.tracer("saga-processor")

    suspend fun runProducer() {
        try {
            consumer.runProducer(buffer, emptyPollDelayMillis)
        } finally {
            buffer.close()
        }
    }

    fun stopPolling() {
        consumer.stopPolling()
    }

    suspend fun runWorker() {
        for (item in buffer) {
            try {
                val execution = item.execution
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
                    continue
                }
                val outcome = Tracing.withSpan(
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
                when (outcome) {
                    ExecutionOutcome.Succeeded,
                    ExecutionOutcome.FailedFinal,
                    ExecutionOutcome.Reenqueued,
                    -> consumer.ack(item)
                }
                metrics.recordProcessed(execution, outcome.toMetricOutcome())
                // Only penalize genuine terminal failures — retries, checkpoints, and async
                // waits are normal Reenqueued outcomes and must NOT count as failures.
                if (outcome == ExecutionOutcome.FailedFinal) {
                    metrics.recordFailed(execution, "failed_final")
                    rateLimiter.recordFailure(execution.definition.name)
                }
                if (outcome == ExecutionOutcome.Reenqueued) {
                    metrics.recordRetried(execution)
                }
            } catch (ex: Exception) {
                // This catch is mutually exclusive with the outcome block above:
                // executor.execute() either returns normally (handled above) or throws (handled here).
                // Both paths call recordFailed but with different reasons — no double-counting.
                logger.warn(
                    "processing failed",
                    kv("sagaId", item.execution.id.toString()),
                    kv("sagaName", item.execution.definition.name),
                    ex
                )
                try {
                    metrics.recordFailed(item.execution, "worker_exception")
                    rateLimiter.recordFailure(item.execution.definition.name)
                } catch (_: Exception) {
                    // ignore
                }
                // Leave in-flight for retry by requeue poller.
            }
        }
    }

    private fun ExecutionOutcome.toMetricOutcome(): String =
        when (this) {
            ExecutionOutcome.Succeeded -> "SUCCEEDED"
            ExecutionOutcome.FailedFinal -> "FAILED_FINAL"
            ExecutionOutcome.Reenqueued -> "REENQUEUED"
        }
}
