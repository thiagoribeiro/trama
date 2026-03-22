package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.testcontainers.DockerClientFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2ESyncSagaTest {

    private lateinit var wm: WireMockServer

    @BeforeAll
    fun checkDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable) {
            println("Docker not available — skipping E2E tests")
            return
        }
        // Warm up containers
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

    // ── V1 definition helpers ─────────────────────────────────────────────────

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    private fun v1Step(name: String, nextName: String? = null) = mapOf(
        "name" to name,
        "up" to mapOf(
            "url" to svc("/step/$name?phase=up"),
            "verb" to "POST",
            "headers" to mapOf("Content-Type" to "application/json"),
            "body" to mapOf("step" to name),
        ),
        "down" to mapOf(
            "url" to svc("/step/$name?phase=down"),
            "verb" to "POST",
            "headers" to mapOf("Content-Type" to "application/json"),
            "body" to mapOf("compensate" to name),
        ),
    )

    private fun v1Definition(name: String, steps: List<String>) = mapOf(
        "definition" to mapOf(
            "name" to name,
            "version" to "v1",
            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 2, "delayMillis" to 50),
            "steps" to steps.map { v1Step(it) },
            "onSuccessCallback" to mapOf(
                "url" to svc("/callback/success"),
                "verb" to "POST",
                "headers" to mapOf("Content-Type" to "application/json"),
                "body" to mapOf("status" to "ok"),
            ),
        ),
        "payload" to mapOf("orderId" to "test-order-001"),
    )

    // ── V2 definition helpers ─────────────────────────────────────────────────

    private fun v2TaskNode(id: String, next: String? = null): Map<String, Any?> {
        val node: MutableMap<String, Any?> = mutableMapOf(
            "kind" to "task",
            "id" to id,
            "action" to mapOf(
                "mode" to "sync",
                "request" to mapOf(
                    "url" to svc("/step/$id?phase=up"),
                    "verb" to "POST",
                    "headers" to mapOf("Content-Type" to "application/json"),
                    "body" to mapOf("step" to id),
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

    private fun v2Definition(name: String, nodeIds: List<String>) = mapOf(
        "definition" to mapOf(
            "name" to name,
            "version" to "v1",
            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 2, "delayMillis" to 50),
            "entrypoint" to nodeIds.first(),
            "nodes" to nodeIds.mapIndexed { i, id -> v2TaskNode(id, nodeIds.getOrNull(i + 1)) },
            "onSuccessCallback" to mapOf(
                "url" to svc("/callback/success"),
                "verb" to "POST",
                "headers" to mapOf("Content-Type" to "application/json"),
                "body" to mapOf("status" to "ok"),
            ),
        ),
        "payload" to mapOf("orderId" to "test-order-001"),
    )

    // ── Test: v1 linear saga succeeds ─────────────────────────────────────────

    @Test
    fun `v1 linear saga all steps succeed`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("v1-linear")

        e2eTest {
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(v1Definition(defName, listOf("reserve", "charge")).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val body = testJson.parseToJsonElement(resp.bodyAsText()).let {
                it as kotlinx.serialization.json.JsonObject
            }
            val sagaId = body["id"]!!.jsonPrimitive.content

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: v2 linear saga succeeds ─────────────────────────────────────────

    @Test
    fun `v2 linear saga all nodes succeed`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("v2-linear")

        e2eTest {
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(v2Definition(defName, listOf("reserve", "charge")).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val body = testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject
            val sagaId = body["id"]!!.jsonPrimitive.content

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: retry on step failure ───────────────────────────────────────────

    @Test
    fun `step failure retried then succeeds`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("retry-success")
        var callCount = 0

        // Return 500 on first call, 200 on subsequent calls for /step/charge?phase=up
        wm.stubFor(
            post(urlPathMatching("/step/charge.*"))
                .willReturn(
                    aResponse()
                        .withStatus(500)
                        .withTransformer("response-template", "enabled", true)
                )
        )
        // Override: first call 500, then 200 — simulate with a counter via WireMock scenarios
        // Simpler approach: remove the 500 stub after the saga starts
        // Actually, use a Scenario-based stub
        wm.resetAll()
        wm.stubSuccessStep("/step/reserve.*")
        wm.stubCallbackEndpoint()

        // Scenario: charge fails first, succeeds second
        wm.stubFor(
            post(urlPathMatching("/step/charge.*"))
                .inScenario("charge-retry")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(500).withBody("""{"error":"transient"}"""))
                .willSetStateTo("Retried")
        )
        wm.stubFor(
            post(urlPathMatching("/step/charge.*"))
                .inScenario("charge-retry")
                .whenScenarioStateIs("Retried")
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("""{"status":"ok"}"""))
        )

        e2eTest {
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(v1Definition(defName, listOf("reserve", "charge")).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val sagaId = (testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject)["id"]!!.jsonPrimitive.content

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            // charge was called at least twice (first failure + retry)
            val chargeRequests = wm.findAll(postRequestedFor(urlPathMatching("/step/charge.*")))
            assertTrue(chargeRequests.size >= 2, "Expected at least 2 calls to charge, got ${chargeRequests.size}")
        }
    }

    // ── Test: retries exhausted triggers compensation ─────────────────────────

    @Test
    fun `retries exhausted triggers compensation and FAILED`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("retries-exhausted")

        wm.resetAll()
        wm.stubSuccessStep("/step/reserve.*")
        wm.stubFor(
            post(urlPathMatching("/step/charge.*"))
                .willReturn(aResponse().withStatus(500).withBody("""{"error":"always fails"}"""))
        )
        wm.stubCallbackEndpoint()

        e2eTest {
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(
                    mapOf(
                        "definition" to mapOf(
                            "name" to defName,
                            "version" to "v1",
                            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
                            "steps" to listOf(v1Step("reserve"), v1Step("charge")),
                            "onFailureCallback" to mapOf(
                                "url" to svc("/callback/failure"),
                                "verb" to "POST",
                                "headers" to mapOf("Content-Type" to "application/json"),
                                "body" to mapOf("status" to "failed"),
                            ),
                        ),
                        "payload" to mapOf("orderId" to "fail-order"),
                    ).toJsonElement().toString()
                )
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val sagaId = (testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject)["id"]!!.jsonPrimitive.content

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("FAILED", final["status"]?.jsonPrimitive?.content)

            // reserve compensation must have been called
            val reserveDown = wm.findAll(postRequestedFor(urlMatching("/step/reserve.*phase=down.*")))
            assertTrue(reserveDown.isNotEmpty(), "Expected reserve compensation to be called")
        }
    }

    // ── Test: compensation in reverse order ───────────────────────────────────

    @Test
    fun `compensation calls happen in reverse node order`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("comp-order")
        val compensationOrder = mutableListOf<String>()

        wm.resetAll()
        wm.stubSuccessStep("/step/step1.*")
        wm.stubSuccessStep("/step/step2.*")
        wm.stubFor(
            post(urlPathMatching("/step/step3.*phase=up.*"))
                .willReturn(aResponse().withStatus(500).withBody("""{"error":"always fails"}"""))
        )
        // Record compensation order via WireMock (it records internally; we check order after)
        wm.stubSuccessStep("/step/step1.*")
        wm.stubSuccessStep("/step/step2.*")
        wm.stubCallbackEndpoint()

        e2eTest {
            val resp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(
                    mapOf(
                        "definition" to mapOf(
                            "name" to defName,
                            "version" to "v1",
                            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
                            "steps" to listOf(v1Step("step1"), v1Step("step2"), v1Step("step3")),
                        ),
                        "payload" to mapOf("orderId" to "comp-order"),
                    ).toJsonElement().toString()
                )
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val sagaId = (testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject)["id"]!!.jsonPrimitive.content

            awaitSagaTerminal(client, sagaId)

            // Collect compensation calls in the order WireMock received them
            val allRequests = wm.allServeEvents.reversed().map { it.request.url }
            val downCalls = allRequests.filter { it.contains("phase=down") }
            assertNotNull(downCalls.find { it.contains("step2") }, "Expected step2 compensation")
            assertNotNull(downCalls.find { it.contains("step1") }, "Expected step1 compensation")

            // step2 should be compensated before step1
            val step2Idx = downCalls.indexOfFirst { it.contains("step2") }
            val step1Idx = downCalls.indexOfFirst { it.contains("step1") }
            assertTrue(step2Idx < step1Idx, "step2 should be compensated before step1 (got step2=$step2Idx, step1=$step1Idx)")
        }
    }
}
