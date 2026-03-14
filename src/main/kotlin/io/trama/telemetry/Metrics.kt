package run.trama.telemetry

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import run.trama.saga.ExecutionState
import run.trama.saga.SagaExecution

class Metrics(
    private val registry: MeterRegistry,
) {
    private val inMemoryQueueSize = AtomicLong(0)
    val queueSizeGauge = registry.gauge("saga.inmemory_queue_size", inMemoryQueueSize)

    fun setQueueSize(size: Long) {
        inMemoryQueueSize.set(size)
    }

    fun recordEnqueued(execution: SagaExecution) {
        registry.counter("saga.enqueue", *tags(execution)).increment()
    }

    fun recordDequeued(execution: SagaExecution) {
        registry.counter("saga.dequeue", *tags(execution)).increment()
    }

    fun recordProcessed(execution: SagaExecution, outcome: String) {
        registry.counter(
            "saga.processed",
            *tags(execution, "outcome", outcome)
        ).increment()
    }

    fun recordFailed(execution: SagaExecution, reason: String) {
        registry.counter(
            "saga.failed",
            *tags(execution, "reason", reason)
        ).increment()
    }

    fun recordRetried(execution: SagaExecution) {
        registry.counter("saga.retried", *tags(execution)).increment()
    }

    fun recordRateLimited(execution: SagaExecution) {
        registry.counter("saga.rate_limited", *tags(execution)).increment()
    }

    fun recordSagaDuration(
        sagaName: String,
        sagaVersion: String,
        finalStatus: String,
        startedAt: Instant,
        endedAt: Instant = Instant.now(),
    ) {
        val durationNanos = kotlin.runCatching {
            java.time.Duration.between(startedAt, endedAt).toNanos()
        }.getOrDefault(0L).coerceAtLeast(0L)
        Timer.builder("saga.duration")
            .description("End-to-end saga duration from startedAt to terminal status")
            .publishPercentileHistogram()
            .tag("saga_name", sagaName)
            .tag("saga_version", sagaVersion)
            .tag("final_status", finalStatus)
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS)
    }

    fun recordStepSuccessDuration(
        sagaName: String,
        sagaVersion: String,
        stepName: String,
        durationNanos: Long,
    ) {
        Timer.builder("saga.step.duration.success")
            .description("Per-step processing duration for successful step calls only")
            .publishPercentileHistogram()
            .tag("saga_name", sagaName)
            .tag("saga_version", sagaVersion)
            .tag("step_name", stepName)
            .register(registry)
            .record(durationNanos.coerceAtLeast(0L), TimeUnit.NANOSECONDS)
    }

    private fun tags(
        execution: SagaExecution,
        vararg extra: String,
    ): Array<String> {
        val tags = mutableListOf(
            "saga_name", execution.definition.name,
            "saga_version", execution.definition.version,
            "phase", phaseTag(execution),
        )
        tags.addAll(extra)
        return tags.toTypedArray()
    }

    private fun phaseTag(execution: SagaExecution): String =
        when (val state = execution.state) {
            is ExecutionState.InProgress -> state.phase.name
            is ExecutionState.Failed -> state.phase.name
            is ExecutionState.Succeeded -> "NA"
        }
}
