package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** onSuccessCallback / onFailureCallback are configured by other E2E suites but never verified there. */
class E2ECallbackHooksTest {
    private lateinit var wm: WireMockServer

    @BeforeEach
    fun setUp() {
        assumeDocker()
        wm = WireMockServer(wireMockConfig().dynamicPort()).also { it.start() }
        wm.stubSuccessStep()
        wm.stubPath("/hooks/success", 200)
        wm.stubPath("/hooks/failure", 200)
    }

    @AfterEach
    fun tearDown() {
        if (::wm.isInitialized) wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    @Test
    fun `success hook receives the rendered body once the saga succeeds`() = e2eTest {
        val def = v2DefinitionMap(
            uniqueName("hook-ok"),
            listOf(taskNodeMap("a", svc("/step/a"))),
            onSuccess = httpCallMap(svc("/hooks/success"), mapOf("sagaId" to "{{sagaId}}", "order" to "{{payload.orderId}}")),
            onFailure = httpCallMap(svc("/hooks/failure")),
        )
        val id = client.runInline(def, mapOf("orderId" to "ord-42"))

        val final = awaitSagaTerminal(client, id)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        val hook = wm.requestsTo("/hooks/success").single()
        val body = testJson.parseToJsonElement(hook.bodyAsString).jsonObject
        assertEquals(id, body["sagaId"]?.jsonPrimitive?.content)
        assertEquals("ord-42", body["order"]?.jsonPrimitive?.content)
        assertEquals(0, wm.requestsTo("/hooks/failure").size)
        assertNull(final["callbackWarning"]?.jsonPrimitive?.content)
    }

    @Test
    fun `failure hook fires after compensations complete`() = e2eTest {
        wm.stubPath("/step/b", 500)
        val def = v2DefinitionMap(
            uniqueName("hook-fail"),
            listOf(
                taskNodeMap("a", svc("/step/a"), next = "b", compensationUrl = svc("/undo/a")),
                taskNodeMap("b", svc("/step/b")),
            ),
            onSuccess = httpCallMap(svc("/hooks/success")),
            onFailure = httpCallMap(svc("/hooks/failure"), mapOf("sagaId" to "{{sagaId}}")),
        )
        wm.stubPath("/undo/a", 200)
        val id = client.runInline(def)

        val final = awaitSagaTerminal(client, id)
        assertEquals("FAILED", final["status"]?.jsonPrimitive?.content)
        assertNotNull(final["failureDescription"]?.jsonPrimitive?.content)
        val hook = wm.requestsTo("/hooks/failure").single()
        val undo = wm.requestsTo("/undo/a").single()
        assertTrue(!hook.loggedDate.before(undo.loggedDate), "failure hook must run after compensation")
        assertEquals(0, wm.requestsTo("/hooks/success").size)
    }

    @Test
    fun `success hook fires when the last node is async`() = e2eTest(wmPort = wm.port()) {
        wm.stubAsyncStep("/async/last")
        val def = v2DefinitionMap(
            uniqueName("hook-async"),
            listOf(
                mapOf(
                    "kind" to "task",
                    "id" to "last",
                    "action" to mapOf(
                        "mode" to "async",
                        "request" to httpCallMap(svc("/async/last"), mapOf("callbackToken" to "{{runtime.callback.token}}")),
                        "acceptedStatusCodes" to listOf(202),
                        "callback" to mapOf("timeoutMillis" to 30_000),
                    ),
                ),
            ),
            onSuccess = httpCallMap(svc("/hooks/success")),
        )
        val id = client.runInline(def)
        awaitSagaStatus(client, id, "WAITING_CALLBACK", timeoutMs = 15_000)
        val token = wm.extractBodyField("/async/last", "callbackToken")
        client.postJson("/workflows/$id/node/last/callback", """{"ok":true}""", headers = mapOf("X-Callback-Token" to token))

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        kotlinx.coroutines.delay(500)
        assertEquals(1, wm.requestsTo("/hooks/success").size, "onSuccessCallback was never called")
    }

    @Test
    fun `failing success hook leaves the saga SUCCEEDED with a callbackWarning`() = e2eTest {
        wm.stubPath("/hooks/success", 503)
        val def = v2DefinitionMap(
            uniqueName("hook-warn"),
            listOf(taskNodeMap("a", svc("/step/a"))),
            onSuccess = httpCallMap(svc("/hooks/success")),
        )
        val id = client.runInline(def)

        awaitSagaTerminal(client, id)
        // callbackWarning is written right after the hook call; poll briefly for it.
        var warning: String? = null
        val deadline = System.currentTimeMillis() + 5_000
        while (warning == null && System.currentTimeMillis() < deadline) {
            val (_, body) = client.getJson("/workflows/$id")
            warning = body?.jsonObject?.get("callbackWarning")?.jsonPrimitive?.content
            if (warning == null) kotlinx.coroutines.delay(100)
        }
        val (_, status) = client.getJson("/workflows/$id")
        assertEquals("SUCCEEDED", status!!.jsonObject["status"]?.jsonPrimitive?.content)
        assertTrue(warning?.contains("503") == true, "expected a callbackWarning mentioning 503, got $warning")
    }
}
