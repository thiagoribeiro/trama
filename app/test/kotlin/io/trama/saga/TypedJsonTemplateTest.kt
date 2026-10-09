package run.trama.saga

import kotlin.test.*
import kotlinx.serialization.json.*

class TypedJsonTemplateTest {
    private val renderer = MustacheTemplateRenderer()
    private val context: Map<String, Any?> = mapOf(
        "payload" to mapOf("number" to 8L, "flag" to false, "nothing" to null,
            "text" to "O'Brien & \"Co\" \\path\nline", "state" to mapOf("minimum" to 7.5, "samples" to listOf(1, 2)),
            "untrusted" to "{{payload.number}}"),
        "nodes" to mapOf("join" to mapOf("response" to mapOf("body" to mapOf("branches" to listOf(
            mapOf("result" to mapOf("output" to mapOf("price" to 111111))),
            mapOf("result" to mapOf("output" to mapOf("price" to 2222)))
        )))))
    )
    private fun typed(body: String): JsonElement = Json.parseToJsonElement(
        renderer.renderTypedJson(TemplateString(body), context)
    )
    @Test fun `whole values preserve scalar and collection types including missing self references`() {
        assertEquals(Json.parseToJsonElement("""{"remaining":8,"flag":false,"null":null,"missing":null,"state":{"minimum":7.5,"samples":[1,2]}}"""),
            typed("""{"remaining":"{{payload.number}}","flag":"{{payload.flag}}","null":"{{payload.nothing}}","missing":"{{nodes.tick.response.body.output.iteration}}","state":"{{payload.state}}"}"""))
    }
    @Test fun `dot and bracket indexes resolve real join output envelopes`() {
        assertEquals(JsonPrimitive(111111), typed(""""{{nodes.join.response.body.branches.0.result.output.price}}""""))
        assertEquals(JsonPrimitive(2222), typed(""""{{nodes.join.response.body.branches[1].result.output.price}}""""))
        assertEquals("BTC 111111 / ETH 2222", typed("""{"text":"BTC {{nodes.join.response.body.branches.0.result.output.price}} / ETH {{nodes.join.response.body.branches.1.result.output.price}}"}""").jsonObject["text"]!!.jsonPrimitive.content)
    }
    @Test fun `values round trip escaped text without interpreting injected placeholders`() {
        assertEquals(JsonPrimitive((context["payload"] as Map<*, *>)["text"] as String), typed(""""{{payload.text}}""""))
        assertEquals(JsonPrimitive("{{payload.number}}"), typed(""""{{payload.untrusted}}""""))
        assertEquals(Json.parseToJsonElement("""{"future":"{{runtime.callback.url}}"}"""), typed("""{"__trama_literal_json__":{"future":"{{runtime.callback.url}}"}}"""))
    }
    @Test fun `legacy mode remains unchanged and typed mode requires explicit version and JSON`() {
        val body = """{"remaining":"{{payload.number}}"}"""
        assertEquals("""{"remaining":"8"}""", renderer.renderBody(httpCall("http://x", body = body), context))
        assertEquals("""{"remaining":8}""", renderer.renderBody(httpCall("http://x", body = body,
            headers = mapOf("X-Trama-Template-Mode" to "typed-json-v1", "Content-Type" to "application/json")), context))
        assertFailsWith<IllegalArgumentException> { renderer.renderBody(httpCall("http://x", body = body,
            headers = mapOf("X-Trama-Template-Mode" to "v2")), context) }
        assertFailsWith<IllegalArgumentException> { renderer.renderBody(httpCall("http://x", body = body,
            headers = mapOf("X-Trama-Template-Mode" to "typed-json-v1", "Content-Type" to "text/plain")), context) }
    }
    @Test fun `unsupported expressions and embedded collections fail without stringifying Java objects`() {
        assertFailsWith<IllegalStateException> { typed(""""state={{payload.state}}"""") }
        assertFailsWith<IllegalArgumentException> { typed(""""{{#payload}}x{{/payload}}"""") }
        assertFailsWith<IllegalStateException> { typed(""""{{payload.number.foo}}"""") }
    }
}
