package run.trama.saga

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import run.trama.saga.redis.ClaimedExecution
import run.trama.saga.redis.SagaExecutionConsumer
import run.trama.saga.redis.SagaRateLimiter
import run.trama.telemetry.Metrics
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SagaExecutionProcessorShutdownTest {
    @Test
    fun `stop polling drains claimed work before workers exit`() = runBlocking {
        val execution = sampleExecution()
        val consumer = TestConsumer(execution)
        val processor = SagaExecutionProcessor(
            consumer = consumer,
            executor = object : SagaExecutor {
                override suspend fun execute(execution: SagaExecution): ExecutionOutcome {
                    consumer.executionStarted.complete(Unit)
                    delay(25)
                    return ExecutionOutcome.Succeeded
                }
            },
            enqueuer = object : SagaEnqueuer {
                override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) = Unit
            },
            rateLimiter = object : SagaRateLimiter {
                override suspend fun checkDelayMillis(sagaName: String): Long? = null
                override suspend fun recordFailure(sagaName: String) = Unit
            },
            metrics = Metrics(SimpleMeterRegistry()),
            bufferSize = 4,
            emptyPollDelayMillis = 10,
        )

        val producerJob = launch { processor.runProducer() }
        val workerJob = launch { processor.runWorker() }

        consumer.executionStarted.await()
        processor.stopPolling()

        withTimeout(1_000) {
            producerJob.join()
            workerJob.join()
        }

        assertTrue(consumer.stopRequested.get())
        assertEquals(1, consumer.ackCount.get())
    }

    private fun sampleExecution(): SagaExecution =
        SagaExecution(
            definition = SagaDefinition(
                name = "order",
                version = "1",
                failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 100),
                steps = listOf(
                    SagaStep(
                        name = "reserve",
                        up = HttpCall(TemplateString("https://example.test/up"), HttpVerb.POST),
                        down = HttpCall(TemplateString("https://example.test/down"), HttpVerb.POST),
                    )
                ),
            ),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
        )

    private class TestConsumer(
        execution: SagaExecution,
    ) : SagaExecutionConsumer {
        private val claim = ClaimedExecution(execution, byteArrayOf(1), 0)
        private val emitted = AtomicBoolean(false)
        val stopRequested = AtomicBoolean(false)
        val ackCount = AtomicInteger(0)
        val executionStarted = CompletableDeferred<Unit>()

        override suspend fun runProducer(
            buffer: kotlinx.coroutines.channels.SendChannel<ClaimedExecution>,
            emptyPollDelayMillis: Long,
        ) {
            if (emitted.compareAndSet(false, true)) {
                buffer.send(claim)
            }
            while (!stopRequested.get()) {
                delay(emptyPollDelayMillis)
            }
        }

        override suspend fun ack(inFlight: ClaimedExecution) {
            if (inFlight == claim) {
                ackCount.incrementAndGet()
            }
        }

        override fun stopPolling() {
            stopRequested.set(true)
        }
    }
}
