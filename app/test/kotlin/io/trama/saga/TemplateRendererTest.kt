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

    private fun render(t: String, context: Map<String, Any?>, escaping: TemplateEscaping = TemplateEscaping.JSON_STRING) =
        renderer.render(TemplateString(t), context, escaping)

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

    private val tricky = mapOf("v" to PayloadValue(JsonPrimitive("O'Brien & \"Co\" <x>\\path\nline")))

    @Test
    fun `JSON bodies escape values as JSON string content`() {
        val out = render("""{"v":"{{payload.v}}"}""", ctx(payload = tricky), TemplateEscaping.JSON_STRING)
        assertEquals("""{"v":"O'Brien & \"Co\" <x>\\path\nline"}""", out)
        // Round-trips through a JSON parser to the original value.
        assertEquals("O'Brien & \"Co\" <x>\\path\nline", Json.parseToJsonElement(out).let { (it as kotlinx.serialization.json.JsonObject)["v"]!!.let { v -> (v as JsonPrimitive).content } })
    }

    @Test
    fun `URLs get the literal value`() {
        val payload = mapOf("q" to PayloadValue(JsonPrimitive("a&b=c")))
        assertEquals("http://x/s?q=a&b=c", render("http://x/s?q={{payload.q}}", ctx(payload = payload), TemplateEscaping.NONE))
    }

    @Test
    fun `header values are literal but can never contain line breaks`() {
        val payload = mapOf("h" to PayloadValue(JsonPrimitive("t=1\r\nX-Injected: yes")))
        assertEquals("t=1X-Injected: yes", render("{{payload.h}}", ctx(payload = payload), TemplateEscaping.HEADER_VALUE))
    }

    @Test
    fun `XML bodies keep entity escaping`() {
        assertEquals("<v>O&#39;Brien &amp; &quot;Co&quot; &lt;x&gt;\\path&#10;line</v>", render("<v>{{payload.v}}</v>", ctx(payload = tricky), TemplateEscaping.XML))
    }

    @Test
    fun `form bodies url-encode values`() {
        val payload = mapOf("q" to PayloadValue(JsonPrimitive("a b&c")))
        assertEquals("q=a+b%26c", render("q={{payload.q}}", ctx(payload = payload), TemplateEscaping.FORM_URLENCODED))
    }

    @Test
    fun `triple mustache is always raw`() {
        val payload = mapOf("q" to PayloadValue(JsonPrimitive("a&\"b")))
        TemplateEscaping.entries.forEach { escaping ->
            assertEquals("a&\"b", render("{{{payload.q}}}", ctx(payload = payload), escaping), "escaping=$escaping")
        }
    }

    @Test
    fun `body escaping follows Content-Type, or the body shape when absent`() {
        fun call(contentType: String?, body: String) =
            httpCall("http://x", body = body, headers = contentType?.let { mapOf("content-type" to it) } ?: emptyMap())
        assertEquals(TemplateEscaping.JSON_STRING, TemplateEscaping.forBody(call("application/json; charset=utf-8", "x")))
        assertEquals(TemplateEscaping.JSON_STRING, TemplateEscaping.forBody(call("application/vnd.api+json", "x")))
        assertEquals(TemplateEscaping.XML, TemplateEscaping.forBody(call("application/soap+xml", "x")))
        assertEquals(TemplateEscaping.FORM_URLENCODED, TemplateEscaping.forBody(call("application/x-www-form-urlencoded", "x")))
        assertEquals(TemplateEscaping.NONE, TemplateEscaping.forBody(call("text/plain", "{}")))
        assertEquals(TemplateEscaping.JSON_STRING, TemplateEscaping.forBody(call(null, """  {"a":1}""")))
        assertEquals(TemplateEscaping.JSON_STRING, TemplateEscaping.forBody(call(null, "[1]")))
        assertEquals(TemplateEscaping.NONE, TemplateEscaping.forBody(call(null, "plain")))
    }
}
