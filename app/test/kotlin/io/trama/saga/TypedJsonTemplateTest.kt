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
    @Test fun `whole values preserve scalar and collection types including explicit null`() {
        assertEquals(Json.parseToJsonElement("""{"remaining":8,"flag":false,"null":null,"state":{"minimum":7.5,"samples":[1,2]}}"""),
            typed("""{"remaining":"{{payload.number}}","flag":"{{payload.flag}}","null":"{{payload.nothing}}","state":"{{payload.state}}"}"""))
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
    @Test fun `JSON bodies always use typed replacement without a mode header`() {
        val body = """{"remaining":"{{payload.number}}"}"""
        assertEquals("""{"remaining":8}""", renderer.renderBody(httpCall("http://x", body = body), context))
        assertEquals("""{"remaining":8}""", renderer.renderBody(httpCall("http://x", body = body,
            headers = mapOf("Content-Type" to "application/json")), context))
        assertEquals("""{"remaining":8}""", renderer.render(TemplateString(body), context, TemplateEscaping.JSON_STRING))
        assertEquals("[8,false,null]", renderer.renderBody(httpCall("http://x", body = """["{{payload.number}}","{{payload.flag}}","{{payload.nothing}}"]"""), context))
        assertEquals("8", renderer.renderBody(httpCall("http://x", body = """"{{payload.number}}"""",
            headers = mapOf("content-type" to "application/vnd.test+json")), context))
    }
    @Test fun `non JSON bodies keep their declared text escaping`() {
        assertEquals("<n>8</n>", renderer.renderBody(httpCall("http://x", body = "<n>{{payload.number}}</n>",
            headers = mapOf("Content-Type" to "application/xml")), context))
        assertEquals("n=8", renderer.renderBody(httpCall("http://x", body = "n={{payload.number}}",
            headers = mapOf("Content-Type" to "application/x-www-form-urlencoded")), context))
        assertEquals("number 8", renderer.renderBody(httpCall("http://x", body = "number {{payload.number}}",
            headers = mapOf("Content-Type" to "text/plain")), context))
        assertNull(renderer.renderBody(httpCall("http://x"), context))
    }
    @Test fun `missing references and out of range indexes identify the JSON field and full reference`() {
        for (reference in listOf("payload.missing", "nodes.tick.response.body.output.iteration",
                "nodes.join.response.body.branches[2].result", "nodes.join.response.body.branches.-1.result")) {
            val failure = assertFailsWith<IllegalArgumentException> {
                typed("""{"input":[{"price":"{{$reference}}"}]}""")
            }
            assertTrue(failure.message!!.contains(reference))
            assertTrue(failure.message!!.contains("$.input[0].price"))
        }
        val failure = assertFailsWith<IllegalArgumentException> {
            typed("""{"text":"Price {{payload.missing}}"}""")
        }
        assertTrue(failure.message!!.contains("$.text"))
    }
    @Test fun `explicit null is a value but traversing it is a reference error`() {
        assertEquals(JsonNull, typed(""""{{payload.nothing}}""""))
        val failure = assertFailsWith<IllegalStateException> { typed(""""{{payload.nothing.value}}"""") }
        assertTrue(failure.message!!.contains("traverses null"))
    }
    @Test fun `invalid JSON bodies fail without falling back to text substitution`() {
        assertFailsWith<IllegalArgumentException> {
            renderer.renderBody(httpCall("http://x", body = "{invalid",
                headers = mapOf("Content-Type" to "application/json")), context)
        }
    }
    @Test fun `unsupported expressions and embedded collections fail without stringifying Java objects`() {
        assertFailsWith<IllegalStateException> { typed(""""state={{payload.state}}"""") }
        assertFailsWith<IllegalArgumentException> { typed(""""{{#payload}}x{{/payload}}"""") }
        assertFailsWith<IllegalArgumentException> { typed(""""{{{payload.number}}}"""") }
        assertFailsWith<IllegalStateException> { typed(""""{{payload.number.foo}}"""") }
    }
}
