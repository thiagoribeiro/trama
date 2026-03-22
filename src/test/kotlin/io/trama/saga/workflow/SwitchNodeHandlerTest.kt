package run.trama.saga.workflow

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.PayloadValue
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.ExecutionState
import run.trama.saga.SagaStep
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SwitchNodeHandlerTest {

    @Test
    fun `first case matches returns first target`() {
        val node = switchNode(
            SwitchCase("pix", condition("input.type", "pix"), "pix-node"),
            SwitchCase("card", condition("input.type", "card"), "card-node"),
            default = "fallback",
        )
        val payload = payloadOf("type" to "pix")

        val result = SwitchNodeHandler.evaluate(node, execution(), payload, emptyList())

        assertEquals("pix-node", result.targetNodeId)
        assertEquals("pix", result.matchedCaseName)
        assertFalse(result.usedDefault)
    }

    @Test
    fun `second case matches when first is false`() {
        val node = switchNode(
            SwitchCase("pix", condition("input.type", "pix"), "pix-node"),
            SwitchCase("card", condition("input.type", "card"), "card-node"),
            default = "fallback",
        )
        val payload = payloadOf("type" to "card")

        val result = SwitchNodeHandler.evaluate(node, execution(), payload, emptyList())

        assertEquals("card-node", result.targetNodeId)
        assertEquals("card", result.matchedCaseName)
        assertFalse(result.usedDefault)
    }

    @Test
    fun `no case matches uses default target`() {
        val node = switchNode(
            SwitchCase("pix", condition("input.type", "pix"), "pix-node"),
            SwitchCase("card", condition("input.type", "card"), "card-node"),
            default = "fallback",
        )
        val payload = payloadOf("type" to "boleto")

        val result = SwitchNodeHandler.evaluate(node, execution(), payload, emptyList())

        assertEquals("fallback", result.targetNodeId)
        assertNull(result.matchedCaseName)
        assertTrue(result.usedDefault)
    }

    @Test
    fun `matching on node response body works`() {
        val node = switchNode(
            SwitchCase("approved", condition("nodes.pay.response.body.status", "approved"), "ship"),
            default = "cancel",
        )
        val stepResults = listOf(
            StepResult(
                index = 0,
                name = "pay",
                upBody = Json.parseToJsonElement("""{"status":"approved"}"""),
                downBody = null,
            )
        )

        val result = SwitchNodeHandler.evaluate(node, execution(), emptyMap(), stepResults)

        assertEquals("ship", result.targetNodeId)
        assertEquals("approved", result.matchedCaseName)
        assertFalse(result.usedDefault)
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** json-logic expression: var == value */
    private fun condition(varPath: String, value: String) =
        Json.parseToJsonElement("""{"==":[{"var":"$varPath"},"$value"]}""")

    private fun switchNode(vararg cases: SwitchCase, default: String) =
        SwitchNode(id = "switch", cases = cases.toList(), defaultTarget = default)

    private fun payloadOf(vararg pairs: Pair<String, String>) =
        pairs.associate { (k, v) -> k to PayloadValue(JsonPrimitive(v)) }

    private fun execution() = SagaExecution(
        definition = SagaDefinition(
            name = "test",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            steps = listOf(SagaStep("s", HttpCall(TemplateString("http://x"), HttpVerb.GET), HttpCall(TemplateString("http://x"), HttpVerb.GET))),
        ),
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = "s"),
        payload = emptyMap(),
    )
}
