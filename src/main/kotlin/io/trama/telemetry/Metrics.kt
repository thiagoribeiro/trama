package io.trama.telemetry

import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicLong

class Metrics(
    private val registry: MeterRegistry,
) {
    val enqueued = registry.counter("saga.enqueue")
    val dequeued = registry.counter("saga.dequeue")
    val processed = registry.counter("saga.processed")
    val failed = registry.counter("saga.failed")
    val retried = registry.counter("saga.retried")
    val rateLimited = registry.counter("saga.rate_limited")

    private val inMemoryQueueSize = AtomicLong(0)
    val queueSizeGauge = registry.gauge("saga.inmemory_queue_size", inMemoryQueueSize)

    fun setQueueSize(size: Long) {
        inMemoryQueueSize.set(size)
    }
}
