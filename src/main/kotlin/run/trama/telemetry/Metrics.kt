package run.trama.telemetry

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import run.trama.saga.ExecutionState
import run.trama.saga.SagaExecution

class Metrics(
    private val registry: MeterRegistry,
) {
    private val inMemoryQueueSize = AtomicLong(0)
    private val redisActivePods = AtomicLong(0)
    private val redisOwnedShards = AtomicLong(0)
    private val redisMembershipRefreshAgeMillis = AtomicLong(0)
    val queueSizeGauge = registry.gauge("saga.inmemory_queue_size", inMemoryQueueSize)
    val redisActivePodsGauge = registry.gauge("saga.redis.active_pods", redisActivePods)
    val redisOwnedShardsGauge = registry.gauge("saga.redis.owned_shards", redisOwnedShards)
    val redisMembershipRefreshAgeGauge =
        registry.gauge("saga.redis.membership_refresh_age_ms", redisMembershipRefreshAgeMillis)

    // Counter + Timer caches — avoids repeated MeterRegistry lookups on the hot path
    private val counterCache = ConcurrentHashMap<String, Counter>()
    private val timerCache = ConcurrentHashMap<String, Timer>()

    private fun counter(name: String, vararg tags: String): Counter =
        counterCache.computeIfAbsent("$name:${tags.joinToString(",")}") {
            registry.counter(name, *tags)
        }

    private fun sagaDurationTimer(sagaName: String, sagaVersion: String, finalStatus: String): Timer =
        timerCache.computeIfAbsent("saga.duration:$sagaName:$sagaVersion:$finalStatus") {
            Timer.builder("saga.duration")
                .description("End-to-end saga duration from startedAt to terminal status")
                .publishPercentileHistogram()
                .tag("saga_name", sagaName)
                .tag("saga_version", sagaVersion)
                .tag("final_status", finalStatus)
                .register(registry)
        }

    private fun stepSuccessTimer(sagaName: String, sagaVersion: String, stepName: String): Timer =
        timerCache.computeIfAbsent("saga.step.duration.success:$sagaName:$sagaVersion:$stepName") {
            Timer.builder("saga.step.duration.success")
                .description("Per-step processing duration for successful step calls only")
                .publishPercentileHistogram()
                .tag("saga_name", sagaName)
                .tag("saga_version", sagaVersion)
                .tag("step_name", stepName)
                .register(registry)
        }

    fun setQueueSize(size: Long) {
        inMemoryQueueSize.set(size)
    }

    fun setRedisActivePods(size: Long) {
        redisActivePods.set(size)
    }

    fun setRedisOwnedShards(size: Long) {
        redisOwnedShards.set(size)
    }

    fun setRedisMembershipRefreshAgeMillis(ageMillis: Long) {
        redisMembershipRefreshAgeMillis.set(ageMillis.coerceAtLeast(0))
    }

    fun recordEnqueued(execution: SagaExecution) {
        counter("saga.enqueue", *tags(execution)).increment()
    }

    fun recordDequeued(execution: SagaExecution) {
        counter("saga.dequeue", *tags(execution)).increment()
    }

    fun recordProcessed(execution: SagaExecution, outcome: String) {
        counter("saga.processed", *tags(execution, "outcome", outcome)).increment()
    }

    fun recordFailed(execution: SagaExecution, reason: String) {
        counter("saga.failed", *tags(execution, "reason", reason)).increment()
    }

    fun recordRetried(execution: SagaExecution) {
        counter("saga.retried", *tags(execution)).increment()
    }

    fun recordRateLimited(execution: SagaExecution) {
        counter("saga.rate_limited", *tags(execution)).increment()
    }

    fun recordRedisClaimScan() {
        counter("saga.redis.claim_scans").increment()
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
        sagaDurationTimer(sagaName, sagaVersion, finalStatus).record(durationNanos, TimeUnit.NANOSECONDS)
    }

    fun recordStepSuccessDuration(
        sagaName: String,
        sagaVersion: String,
        stepName: String,
        durationNanos: Long,
    ) {
        stepSuccessTimer(sagaName, sagaVersion, stepName).record(durationNanos.coerceAtLeast(0L), TimeUnit.NANOSECONDS)
    }

    /** Incremented each time an async task node enters WAITING_CALLBACK state. */
    fun recordCallbackWaitEntered(sagaName: String, sagaVersion: String) {
        counter("saga.callback.wait_entered", "saga_name", sagaName, "saga_version", sagaVersion).increment()
    }

    /** Incremented when a callback arrives at the receiver endpoint. result = "accepted" | "rejected". */
    fun recordCallbackReceived(sagaName: String, sagaVersion: String, result: String) {
        counter("saga.callback.received", "saga_name", sagaName, "saga_version", sagaVersion, "result", result).increment()
    }

    /** Incremented when a callback is rejected. reason = "expired" | "invalid_signature" | "replay" | "wrong_attempt" | "no_waiting_entry". */
    fun recordCallbackRejected(sagaName: String, sagaVersion: String, reason: String) {
        counter("saga.callback.rejected", "saga_name", sagaName, "saga_version", sagaVersion, "reason", reason).increment()
    }

    /** Incremented when a WAITING_CALLBACK execution times out (sentinel consumed without prior callback). */
    fun recordCallbackTimeout(sagaName: String, sagaVersion: String) {
        counter("saga.callback.timeout", "saga_name", sagaName, "saga_version", sagaVersion).increment()
    }

    /** Incremented each time a switch node is evaluated. result = "case" | "default". */
    fun recordSwitchEvaluated(sagaName: String, sagaVersion: String, result: String) {
        counter("saga.switch.evaluated", "saga_name", sagaName, "saga_version", sagaVersion, "result", result).increment()
    }

    /** Incremented by the Postgres fallback scanner for each execution re-enqueued due to expired callback. */
    fun recordCallbackTimeoutScan(requeued: Int) {
        counter("saga.callback.timeout_scan_requeued").increment(requeued.toDouble())
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
            is ExecutionState.InProgress -> state.phase?.name ?: "UP"
            is ExecutionState.Compensating -> "DOWN"
            is ExecutionState.Failed -> state.phase?.name ?: "NA"
            is ExecutionState.Succeeded -> "NA"
            is ExecutionState.WaitingCallback -> "WAITING"
        }
}
