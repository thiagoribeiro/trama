package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking

class SagaExecutionQueueTest {
    @Test
    fun `enqueue and receive`() = runBlocking {
        val queue = SagaExecutionQueue()
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "test",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = emptyList(),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = java.util.UUID.randomUUID(),
            startedAt = java.time.Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = emptyMap(),
        )

        queue.enqueue(exec)
        val received = queue.receive()
        assertEquals(exec.id, received.id)
    }

    @Test
    fun `tryReceive returns null when empty`() {
        val queue = SagaExecutionQueue()
        assertNull(queue.tryReceive())
    }

    @Test
    fun `tryReceive returns value when available`() = runBlocking {
        val queue = SagaExecutionQueue()
        val exec = SagaExecution(
            definition = SagaDefinition(
                name = "test",
                version = "1",
                failureHandling = FailureHandling.Retry(1, 10),
                steps = emptyList(),
                onSuccessCallback = null,
                onFailureCallback = null,
            ),
            id = java.util.UUID.randomUUID(),
            startedAt = java.time.Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = emptyMap(),
        )
        queue.enqueue(exec)
        val received = queue.tryReceive()
        assertNotNull(received)
        assertEquals(exec.id, received.id)
    }
}
