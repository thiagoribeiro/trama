package run.trama.saga

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.serialization.json.JsonPrimitive

class TemplateContextBuilderTest {

    private val exec = SagaExecution(
        definition = SagaDefinition(
            name = "test-saga",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 1),
            steps = emptyList(),
        ),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = null),
    )

    @Test
    fun `prev reflects last completed step`() {
        val results = listOf(
            StepResult(0, "checkout", JsonPrimitive("body_checkout"), null),
            StepResult(1, "payment",  JsonPrimitive("body_payment"),  null),
        )
        val ctx = TemplateContextBuilder.build(exec, "payment", ExecutionPhase.UP, results)
        @Suppress("UNCHECKED_CAST")
        val prev = ctx["prev"] as Map<String, Any?>
        assertEquals("body_payment", prev["body"])
    }

    @Test
    fun `steps list is in index order`() {
        val results = listOf(
            StepResult(0, "checkout", JsonPrimitive("body_checkout"), null),
            StepResult(1, "payment",  JsonPrimitive("body_payment"),  null),
        )
        val ctx = TemplateContextBuilder.build(exec, "payment", ExecutionPhase.UP, results)
        @Suppress("UNCHECKED_CAST")
        val steps = ctx["steps"] as List<Map<String, Any?>>
        assertEquals("checkout", steps[0]["name"])
        assertEquals("payment",  steps[1]["name"])
    }

    @Test
    fun `step lookup by name uses newest entry when step name is repeated at different indices`() {
        val results = listOf(
            StepResult(0, "checkout", JsonPrimitive("OLD"), null),
            StepResult(2, "checkout", JsonPrimitive("NEW"), null),
        )
        val ctx = TemplateContextBuilder.build(exec, "noop", ExecutionPhase.UP, results)
        @Suppress("UNCHECKED_CAST")
        val step = ctx["step"] as Map<String, Any?>
        @Suppress("UNCHECKED_CAST")
        val checkout = step["checkout"] as Map<String, Any?>
        assertEquals("NEW", checkout["body"])
    }
}
