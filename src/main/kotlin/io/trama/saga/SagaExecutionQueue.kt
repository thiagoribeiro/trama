package io.trama.saga

import kotlinx.coroutines.channels.Channel

class SagaExecutionQueue(
    capacity: Int = Channel.BUFFERED,
) {
    private val channel = Channel<SagaExecution>(capacity)
    private val size = java.util.concurrent.atomic.AtomicInteger(0)

    suspend fun enqueue(execution: SagaExecution) {
        size.incrementAndGet()
        channel.send(execution)
    }

    suspend fun receive(): SagaExecution {
        val value = channel.receive()
        size.decrementAndGet()
        return value
    }

    fun tryReceive(): SagaExecution? {
        val result = channel.tryReceive().getOrNull()
        if (result != null) {
            size.decrementAndGet()
        }
        return result
    }

    fun size(): Int = size.get()
}
