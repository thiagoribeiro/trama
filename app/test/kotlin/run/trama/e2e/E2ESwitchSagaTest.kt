package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.DockerClientFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2ESwitchSagaTest {

    private lateinit var wm: WireMockServer

    @BeforeAll
    fun checkDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        E2EContainers.postgres
        E2EContainers.redis
    }

    @BeforeEach
    fun startWiremock() {
        wm = WireMockServer(wireMockConfig().dynamicPort())
        wm.start()
        wm.stubSuccessStep()
        wm.stubCallbackEndpoint()
    }

    @AfterEach
    fun stopWiremock() {
        wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    /**
     * Builds a v2 definition with a switch node routing by [payload.paymentMethod].
     * Routes: pix → pix-payment → notify, card → card-payment → notify, default → fallback-payment → notify.
     */
    private fun switchDefinition(name: String): Map<*, *> {
        fun taskNode(id: String, next: String? = null): Map<String, Any?> {
            val node: MutableMap<String, Any?> = mutableMapOf(
                "kind" to "task",
                "id" to id,
                "action" to mapOf(
                    "mode" to "sync",
                    "request" to mapOf(
                        "url" to svc("/step/$id?phase=up"),
                        "verb" to "POST",
                        "headers" to mapOf("Content-Type" to "application/json"),
                        "body" to mapOf("node" to id, "method" to "{{payload.paymentMethod}}"),
                    ),
                ),
                "compensation" to mapOf(
                    "url" to svc("/step/$id?phase=down"),
                    "verb" to "POST",
                    "headers" to mapOf("Content-Type" to "application/json"),
                    "body" to mapOf("compensate" to id),
                ),
            )
            if (next != null) node["next"] = next
            return node
        }

        return mapOf(
            "definition" to mapOf(
                "name" to name,
                "version" to "v1",
                "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
                "entrypoint" to "choose-payment",
                "nodes" to listOf(
                    mapOf(
                        "kind" to "switch",
                        "id" to "choose-payment",
                        "cases" to listOf(
                            mapOf("name" to "pix", "when" to mapOf("==" to listOf(mapOf("var" to "payload.paymentMethod"), "pix")), "target" to "pix-payment"),
                            mapOf("name" to "card", "when" to mapOf("==" to listOf(mapOf("var" to "payload.paymentMethod"), "card")), "target" to "card-payment"),
                        ),
                        "default" to "fallback-payment",
                    ),
                    taskNode("pix-payment", next = "notify"),
                    taskNode("card-payment", next = "notify"),
                    taskNode("fallback-payment", next = "notify"),
                    taskNode("notify"),
                ),
                "onSuccessCallback" to mapOf(
                    "url" to svc("/callback/success"),
                    "verb" to "POST",
                    "headers" to mapOf("Content-Type" to "application/json"),
                    "body" to mapOf("status" to "ok"),
                ),
            ),
            "payload" to mapOf("paymentMethod" to "pix", "orderId" to "ord-001"),
        )
    }

    private suspend fun runSwitchSaga(
        client: io.ktor.client.HttpClient,
        name: String,
        paymentMethod: String,
    ): String {
        val def = switchDefinition(name).toMutableMap()
        @Suppress("UNCHECKED_CAST")
        (def as MutableMap<String, Any?>)["payload"] = mapOf("paymentMethod" to paymentMethod, "orderId" to "ord-test")
        val resp = client.post("/sagas/run") {
            contentType(ContentType.Application.Json)
            setBody(def.toJsonElement().toString())
        }
        assertEquals(200, resp.status.value, resp.bodyAsText())
        return (testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject)["id"]!!.jsonPrimitive.content
    }

    // ── Test: pix branch selected ─────────────────────────────────────────────

    @Test
    fun `switch routes to pix branch when paymentMethod is pix`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("switch-pix")

        e2eTest(wmPort = wm.port()) {
            val sagaId = runSwitchSaga(client, defName, "pix")
            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            val upRequests = wm.findAll(postRequestedFor(urlMatching("/step/.*[?&]phase=up.*")))
                .map { it.url }
            assertTrue(upRequests.any { it.contains("pix-payment") }, "Expected pix-payment to be called")
            assertTrue(upRequests.none { it.contains("card-payment") }, "Expected card-payment NOT to be called")
            assertTrue(upRequests.none { it.contains("fallback-payment") }, "Expected fallback-payment NOT to be called")
        }
    }

    // ── Test: card branch selected ────────────────────────────────────────────

    @Test
    fun `switch routes to card branch when paymentMethod is card`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("switch-card")

        e2eTest(wmPort = wm.port()) {
            val sagaId = runSwitchSaga(client, defName, "card")
            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            val upRequests = wm.findAll(postRequestedFor(urlMatching("/step/.*[?&]phase=up.*")))
                .map { it.url }
            assertTrue(upRequests.any { it.contains("card-payment") }, "Expected card-payment to be called")
            assertTrue(upRequests.none { it.contains("pix-payment") }, "Expected pix-payment NOT to be called")
        }
    }

    // ── Test: default branch selected ─────────────────────────────────────────

    @Test
    fun `switch routes to default branch for unknown paymentMethod`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("switch-default")

        e2eTest(wmPort = wm.port()) {
            val sagaId = runSwitchSaga(client, defName, "boleto")
            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            val upRequests = wm.findAll(postRequestedFor(urlMatching("/step/.*[?&]phase=up.*")))
                .map { it.url }
            assertTrue(upRequests.any { it.contains("fallback-payment") }, "Expected fallback-payment (default) to be called")
            assertTrue(upRequests.none { it.contains("pix-payment") }, "Expected pix-payment NOT to be called")
            assertTrue(upRequests.none { it.contains("card-payment") }, "Expected card-payment NOT to be called")
        }
    }

    // ── Test: invalid definition rejected ─────────────────────────────────────

    @Test
    fun `switch definition without default field is rejected with 400`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return

        e2eTest(wmPort = wm.port()) {
            val invalidDef = mapOf(
                "definition" to mapOf(
                    "name" to uniqueName("invalid-switch"),
                    "version" to "v1",
                    "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
                    "entrypoint" to "choose",
                    "nodes" to listOf(
                        mapOf(
                            "kind" to "switch",
                            "id" to "choose",
                            // No "default" field — should be rejected
                            "cases" to listOf(
                                mapOf("name" to "a", "when" to mapOf("==" to listOf(mapOf("var" to "payload.x"), "1")), "target" to "task1"),
                            ),
                        ),
                        mapOf(
                            "kind" to "task",
                            "id" to "task1",
                            "action" to mapOf(
                                "mode" to "sync",
                                "request" to mapOf(
                                    "url" to svc("/step/task1"),
                                    "verb" to "POST",
                                ),
                            ),
                        ),
                    ),
                ),
                "payload" to emptyMap<String, String>(),
            )
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(invalidDef.toJsonElement().toString())
            }
            assertTrue(
                resp.status.value in 400..422,
                "Expected 4xx for invalid definition, got ${resp.status.value}: ${resp.bodyAsText()}"
            )
        }
    }
}
