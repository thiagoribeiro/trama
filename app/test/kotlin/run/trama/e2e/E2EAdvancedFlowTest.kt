package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import kotlinx.coroutines.delay
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Backoff timing, templating across nodes, async successWhen, and richer split/join shapes. */
class E2EAdvancedFlowTest {
    private lateinit var wm: WireMockServer

    @BeforeEach
    fun setUp() {
        assumeDocker()
        wm = WireMockServer(wireMockConfig().dynamicPort()).also { it.start() }
        wm.stubSuccessStep()
    }

    @AfterEach
    fun tearDown() {
        if (::wm.isInitialized) wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    private fun asyncNodeMap(id: String, next: String? = null, successWhen: Any? = null) = buildMap<String, Any?> {
        put("kind", "task")
        put("id", id)
        put(
            "action",
            buildMap {
                put("mode", "async")
                put("request", httpCallMap(svc("/async/$id"), mapOf("callbackUrl" to "{{runtime.callback.url}}", "callbackToken" to "{{runtime.callback.token}}")))
                put("acceptedStatusCodes", listOf(202))
                put("callback", buildMap { put("timeoutMillis", 30_000); if (successWhen != null) put("successWhen", successWhen) })
            },
        )
        if (next != null) put("next", next)
    }

    private suspend fun awaitRequest(path: String, timeoutMs: Long = 10_000) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (wm.requestsTo(path).isEmpty()) {
            check(System.currentTimeMillis() < deadline) { "no request reached $path" }
            delay(50)
        }
    }

    /** Fires the callback captured at [asyncPath], using the URL and token the saga sent there. */
    private suspend fun io.ktor.client.HttpClient.fireCallback(asyncPath: String, body: String) {
        val url = wm.extractBodyField(asyncPath, "callbackUrl")
        val token = wm.extractBodyField(asyncPath, "callbackToken")
        val resp = postJson(java.net.URI(url).path, body, headers = mapOf("X-Callback-Token" to token))
        check(resp.status.value in 200..202) { "callback rejected: ${resp.status.value}" }
    }

    // ── Backoff ─────────────────────────────────────────────────────────────

    @Test
    fun `backoff retries maxAttempts times with growing delays`() = e2eTest {
        wm.stubPath("/fail/x", 500)
        val def = v2DefinitionMap(
            uniqueName("backoff"),
            listOf(taskNodeMap("x", svc("/fail/x"))),
            failureHandling = mapOf("type" to "backoff", "maxAttempts" to 2, "initialDelayMillis" to 300, "maxDelayMillis" to 5_000, "multiplier" to 2.0),
        )
        val id = client.runInline(def)

        assertEquals("FAILED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        val times = wm.requestsTo("/fail/x").map { it.loggedDate.time }.sorted()
        assertEquals(3, times.size, "initial call + 2 retries")
        val gap1 = times[1] - times[0]
        val gap2 = times[2] - times[1]
        assertTrue(gap1 >= 250, "first retry after ${gap1}ms, expected ~300ms")
        assertTrue(gap2 >= 550, "second retry after ${gap2}ms, expected ~600ms")
    }

    // ── Templating ──────────────────────────────────────────────────────────

    @Test
    fun `url, headers and body are rendered from payload and previous node responses`() = e2eTest {
        wm.stubPath("/step/a", 200, """{"id":"A-1","nested":{"v":7}}""")
        wm.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.post(com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching("/orders/.*"))
                .willReturn(com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(200).withBody("{}")),
        )
        val def = v2DefinitionMap(
            uniqueName("tmpl"),
            listOf(
                taskNodeMap("a", svc("/step/a"), next = "b"),
                taskNodeMap(
                    "b",
                    svc("/orders/{{payload.orderId}}"),
                    body = mapOf("fromA" to "{{nodes.a.response.body.id}}", "prev" to "{{prev.body.nested.v}}"),
                    headers = mapOf("Content-Type" to "application/json", "X-Tenant" to "{{payload.tenant}}"),
                ),
            ),
        )
        val id = client.runInline(def, mapOf("orderId" to "o-3", "tenant" to "acme"))

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        val b = wm.requestsTo("/orders/o-3").single()
        assertEquals("acme", b.getHeader("X-Tenant"))
        val body = testJson.parseToJsonElement(b.bodyAsString).jsonObject
        assertEquals("A-1", body["fromA"]?.jsonPrimitive?.content)
        assertEquals("7", body["prev"]?.jsonPrimitive?.content)
    }

    @Test
    fun `JSON bodies carry values with quotes, backslashes and newlines intact`() = e2eTest {
        val tricky = "O'Brien & \"Co\" <x> \\path\nline"
        val def = v2DefinitionMap(uniqueName("escape"), listOf(taskNodeMap("a", svc("/step/a"), body = mapOf("name" to "{{payload.name}}"))))
        val id = client.runInline(def, mapOf("name" to tricky))

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        val received = testJson.parseToJsonElement(wm.requestsTo("/step/a").single().bodyAsString).jsonObject
        assertEquals(tricky, received["name"]?.jsonPrimitive?.content)
    }

    @Test
    fun `switch conditions on input route like payload`() = e2eTest {
        val def = v2DefinitionMap(
            uniqueName("switch-input"),
            listOf(
                mapOf(
                    "kind" to "switch",
                    "id" to "route",
                    "cases" to listOf(mapOf("name" to "pix", "when" to mapOf("==" to listOf(mapOf("var" to "input.paymentMethod"), "pix")), "target" to "pix")),
                    "default" to "fallback",
                ),
                taskNodeMap("pix", svc("/step/pix")),
                taskNodeMap("fallback", svc("/step/fallback")),
            ),
        )
        val id = client.runInline(def, mapOf("paymentMethod" to "pix"))

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/pix").size, "input.paymentMethod must match the payload")
        assertEquals(0, wm.requestsTo("/step/fallback").size)
    }

    // ── Async successWhen ───────────────────────────────────────────────────

    private val approved = mapOf("==" to listOf(mapOf("var" to "callback.body.status"), "approved"))

    @Test
    fun `async callback matching successWhen resumes the saga`() = e2eTest(wmPort = wm.port()) {
        wm.stubAsyncStep("/async/auth")
        val id = client.runInline(v2DefinitionMap(uniqueName("sw-ok"), listOf(asyncNodeMap("auth", next = "done", successWhen = approved), taskNodeMap("done", svc("/step/done")))))
        awaitRequest("/async/auth")
        delay(300)

        client.fireCallback("/async/auth", """{"status":"approved"}""")

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/done").size)
    }

    @Test
    fun `async callback not matching successWhen fails the saga`() = e2eTest(wmPort = wm.port()) {
        wm.stubAsyncStep("/async/auth")
        val id = client.runInline(v2DefinitionMap(uniqueName("sw-no"), listOf(asyncNodeMap("auth", next = "done", successWhen = approved), taskNodeMap("done", svc("/step/done")))))
        awaitRequest("/async/auth")
        delay(300)

        client.fireCallback("/async/auth", """{"status":"declined"}""")

        assertEquals("FAILED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(0, wm.requestsTo("/step/done").size)
    }

    // ── Split / join shapes ─────────────────────────────────────────────────

    @Test
    fun `nested split inside a branch joins inner and outer barriers`() = e2eTest {
        val def = v2DefinitionMap(
            uniqueName("nested-split"),
            listOf(
                mapOf("kind" to "split", "id" to "outer", "branches" to listOf("left", "inner"), "join" to "outer-join"),
                taskNodeMap("left", svc("/step/left")),
                mapOf("kind" to "split", "id" to "inner", "branches" to listOf("i1", "i2"), "join" to "inner-join"),
                taskNodeMap("i1", svc("/step/i1")),
                taskNodeMap("i2", svc("/step/i2")),
                mapOf("kind" to "join", "id" to "inner-join", "next" to "after-inner"),
                taskNodeMap("after-inner", svc("/step/after-inner")),
                mapOf("kind" to "join", "id" to "outer-join", "next" to "end"),
                taskNodeMap("end", svc("/step/end")),
            ),
        )
        val id = client.runInline(def)

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        listOf("left", "i1", "i2", "after-inner", "end").forEach {
            assertEquals(1, wm.requestsTo("/step/$it").size, "node $it should run exactly once")
        }
        val afterInner = wm.requestsTo("/step/after-inner").single().loggedDate
        val end = wm.requestsTo("/step/end").single().loggedDate
        assertTrue(!end.before(afterInner), "outer join must wait for the inner branch to finish")
    }

    @Test
    fun `join waits for a sleeping branch`() = e2eTest {
        val def = v2DefinitionMap(
            uniqueName("split-sleep"),
            listOf(
                mapOf("kind" to "split", "id" to "fan", "branches" to listOf("fast", "slow"), "join" to "fan-in"),
                taskNodeMap("fast", svc("/step/fast")),
                sleepNodeMap("slow", 1_500, next = "slow-task"),
                taskNodeMap("slow-task", svc("/step/slow-task")),
                mapOf("kind" to "join", "id" to "fan-in", "next" to "end"),
                taskNodeMap("end", svc("/step/end")),
            ),
        )
        val id = client.runInline(def)

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id, timeoutMs = 20_000)["status"]?.jsonPrimitive?.content)
        val fast = wm.requestsTo("/step/fast").single().loggedDate.time
        val end = wm.requestsTo("/step/end").single().loggedDate.time
        assertTrue(end - fast >= 1_400, "join fired ${end - fast}ms after the fast branch; the slow branch sleeps 1500ms")
    }

    @Test
    fun `join waits for an async branch until its callback arrives`() = e2eTest(wmPort = wm.port()) {
        wm.stubAsyncStep("/async/ext")
        val def = v2DefinitionMap(
            uniqueName("split-async"),
            listOf(
                mapOf("kind" to "split", "id" to "fan", "branches" to listOf("local", "ext"), "join" to "fan-in"),
                taskNodeMap("local", svc("/step/local")),
                asyncNodeMap("ext", next = "ext-after"),
                taskNodeMap("ext-after", svc("/step/ext-after")),
                mapOf("kind" to "join", "id" to "fan-in", "next" to "end"),
                taskNodeMap("end", svc("/step/end")),
            ),
        )
        val id = client.runInline(def)
        awaitRequest("/async/ext")
        awaitRequest("/step/local")
        delay(1_000)
        assertEquals(0, wm.requestsTo("/step/end").size, "join must not fire before the async branch is called back")

        client.fireCallback("/async/ext", """{"ok":true}""")

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/end").size)
    }

    @Test
    fun `join fires when a branch ends with an async node`() = e2eTest(wmPort = wm.port()) {
        wm.stubAsyncStep("/async/ext")
        val def = v2DefinitionMap(
            uniqueName("split-async-last"),
            listOf(
                mapOf("kind" to "split", "id" to "fan", "branches" to listOf("local", "ext"), "join" to "fan-in"),
                taskNodeMap("local", svc("/step/local")),
                asyncNodeMap("ext"),
                mapOf("kind" to "join", "id" to "fan-in", "next" to "end"),
                taskNodeMap("end", svc("/step/end")),
            ),
        )
        val id = client.runInline(def)
        awaitRequest("/async/ext")
        awaitRequest("/step/local")
        delay(1_000)
        assertEquals(0, wm.requestsTo("/step/end").size, "join must not fire before the async branch is called back")

        client.fireCallback("/async/ext", """{"ok":true}""")

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/end").size)
    }
}
