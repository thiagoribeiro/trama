package run.trama.saga

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive

class TemplateRendererTest {
    private val renderer = MustacheTemplateRenderer()

    private val execution = SagaExecution(
        definition = SagaDefinition("orders", "7", FailureHandling.Retry(1, 1), steps = emptyList()),
        id = UUID.fromString("00000000-0000-0000-0000-000000000042"),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = "a"),
    )

    private fun ctx(
        steps: List<StepResult> = emptyList(),
        payload: Map<String, PayloadValue> = emptyMap(),
    ) = TemplateContextBuilder.build(execution, "current", ExecutionPhase.UP, steps, payload)

    private fun render(t: String, context: Map<String, Any?>) = renderer.render(TemplateString(t), context)

    @Test
    fun `saga metadata is available`() {
        assertEquals(
            "orders/7/00000000-0000-0000-0000-000000000042/current/UP",
            render("{{sagaName}}/{{sagaVersion}}/{{sagaId}}/{{stepName}}/{{phase}}", ctx()),
        )
    }

    @Test
    fun `payload scalars and nested objects render`() {
        val payload = mapOf(
            "userId" to PayloadValue(JsonPrimitive("u-1")),
            "amount" to PayloadValue(JsonPrimitive(99)),
            "address" to PayloadValue(Json.parseToJsonElement("""{"city":"Recife"}""")),
        )
        assertEquals("u-1|99|Recife", render("{{payload.userId}}|{{payload.amount}}|{{payload.address.city}}", ctx(payload = payload)))
    }

    @Test
    fun `prev, step by name, step by index and nodes resolve previous bodies`() {
        val steps = listOf(
            StepResult(0, "reserve", Json.parseToJsonElement("""{"reservationId":"r-1"}"""), null),
            StepResult(1, "charge", Json.parseToJsonElement("""{"chargeId":"c-9"}"""), null),
        )
        val c = ctx(steps)
        assertEquals("c-9", render("{{prev.body.chargeId}}", c))
        assertEquals("r-1", render("{{step.reserve.body.reservationId}}", c))
        assertEquals("r-1", render("{{step.0.body.reservationId}}", c))
        assertEquals("c-9", render("{{nodes.charge.response.body.chargeId}}", c))
    }

    @Test
    fun `missing variables render as empty string`() {
        assertEquals("[]", render("[{{payload.nope}}]", ctx()))
        assertEquals("[]", render("[{{prev.body.x}}]", ctx()))
    }

    @Test
    fun `sections iterate over steps list`() {
        val steps = listOf(
            StepResult(0, "a", null, null),
            StepResult(1, "b", null, null),
        )
        assertEquals("a,b,", render("{{#steps}}{{name}},{{/steps}}", ctx(steps)))
    }

    @Test
    fun `triple mustache does not html-escape`() {
        val payload = mapOf("q" to PayloadValue(JsonPrimitive("a&b<c>")))
        assertEquals("a&b<c>", render("{{{payload.q}}}", ctx(payload = payload)))
    }

    @Test
    fun `double mustache html-escapes values`() {
        // Documents current behavior: Mustache escapes by default, which matters when values
        // are interpolated into JSON bodies (quotes become &quot;). Use {{{ }}} for raw values.
        val payload = mapOf("q" to PayloadValue(JsonPrimitive("say \"hi\"")))
        assertEquals("say &quot;hi&quot;", render("{{payload.q}}", ctx(payload = payload)))
    }
}
