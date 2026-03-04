package io.trama.saga

import io.trama.saga.redis.SagaExecutionRedisConsumer
import io.trama.saga.redis.SagaRateLimiter
import io.trama.telemetry.Metrics
import io.trama.telemetry.Tracing
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
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
    private val consumer: SagaExecutionRedisConsumer,
    private val executor: SagaExecutor,
    private val enqueuer: SagaEnqueuer,
    private val rateLimiter: SagaRateLimiter,
    private val metrics: Metrics,
    bufferSize: Int = 200,
    private val emptyPollDelayMillis: Long = 50,
) {
    private val buffer = Channel<SagaExecutionRedisConsumer.InFlightExecution>(bufferSize)
    private val logger = LoggerFactory.getLogger(SagaExecutionProcessor::class.java)
    private val tracer = Tracing.tracer("saga-processor")

    suspend fun runProducer() {
        while (true) {
            val items = consumer.pollReady()
            if (items.isEmpty()) {
                delay(emptyPollDelayMillis)
                continue
            }
            for (item in items) {
                buffer.send(item)
            }
        }
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
                    metrics.rateLimited.increment()
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
                metrics.processed.increment()
                if (outcome != ExecutionOutcome.Succeeded) {
                    metrics.failed.increment()
                    rateLimiter.recordFailure(execution.definition.name)
                }
                if (outcome == ExecutionOutcome.Reenqueued) {
                    metrics.retried.increment()
                }
            } catch (ex: Exception) {
                logger.warn(
                    "processing failed",
                    kv("sagaId", item.execution.id.toString()),
                    kv("sagaName", item.execution.definition.name),
                    ex
                )
                try {
                    metrics.failed.increment()
                    rateLimiter.recordFailure(item.execution.definition.name)
                } catch (_: Exception) {
                    // ignore
                }
                // Leave in-flight for retry by requeue poller.
            }
        }
    }
}
