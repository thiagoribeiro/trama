package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.delay
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
class E2EAsyncCallbackTest {

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
        wm.stubAsyncStep("/async-step/authorize")
        wm.stubCallbackEndpoint()
    }

    @AfterEach
    fun stopWiremock() {
        wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    /**
     * V2 definition: authorize (async) → capture (sync) → notify (sync)
     * The async node injects callbackUrl and callbackToken into the request body.
     */
    private fun asyncDefinition(
        name: String,
        callbackTimeoutMillis: Long = 30_000,
    ) = mapOf(
        "definition" to mapOf(
            "name" to name,
            "version" to "v1",
            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
            "entrypoint" to "authorize",
            "nodes" to listOf(
                mapOf(
                    "kind" to "task",
                    "id" to "authorize",
                    "action" to mapOf(
                        "mode" to "async",
                        "request" to mapOf(
                            "url" to svc("/async-step/authorize"),
                            "verb" to "POST",
                            "headers" to mapOf("Content-Type" to "application/json"),
                            "body" to mapOf(
                                "orderId" to "{{payload.orderId}}",
                                "callbackUrl" to "{{runtime.callback.url}}",
                                "callbackToken" to "{{runtime.callback.token}}",
                            ),
                        ),
                        "acceptedStatusCodes" to listOf(202),
                        "callback" to mapOf("timeoutMillis" to callbackTimeoutMillis),
                    ),
                    "compensation" to mapOf(
                        "url" to svc("/step/authorize?phase=down"),
                        "verb" to "POST",
                        "headers" to mapOf("Content-Type" to "application/json"),
                        "body" to mapOf("compensate" to "authorize"),
                    ),
                    "next" to "capture",
                ),
                mapOf(
                    "kind" to "task",
                    "id" to "capture",
                    "action" to mapOf(
                        "mode" to "sync",
                        "request" to mapOf(
                            "url" to svc("/step/capture?phase=up"),
                            "verb" to "POST",
                            "headers" to mapOf("Content-Type" to "application/json"),
                            "body" to mapOf("step" to "capture"),
                        ),
                    ),
                    "compensation" to mapOf(
                        "url" to svc("/step/capture?phase=down"),
                        "verb" to "POST",
                        "headers" to mapOf("Content-Type" to "application/json"),
                        "body" to mapOf("compensate" to "capture"),
                    ),
                    "next" to "notify",
                ),
                mapOf(
                    "kind" to "task",
                    "id" to "notify",
                    "action" to mapOf(
                        "mode" to "sync",
                        "request" to mapOf(
                            "url" to svc("/step/notify?phase=up"),
                            "verb" to "POST",
                            "headers" to mapOf("Content-Type" to "application/json"),
                            "body" to mapOf("step" to "notify"),
                        ),
                    ),
                ),
            ),
            "onSuccessCallback" to mapOf(
                "url" to svc("/callback/success"),
                "verb" to "POST",
                "headers" to mapOf("Content-Type" to "application/json"),
                "body" to mapOf("status" to "ok"),
            ),
            "onFailureCallback" to mapOf(
                "url" to svc("/callback/failure"),
                "verb" to "POST",
                "headers" to mapOf("Content-Type" to "application/json"),
                "body" to mapOf("status" to "failed"),
            ),
        ),
        "payload" to mapOf("orderId" to "ord-async-001"),
    )

    // ── Helper: start an async saga and return its ID ─────────────────────────

    private suspend fun startAsyncSaga(
        client: io.ktor.client.HttpClient,
        defName: String,
        timeoutMillis: Long = 30_000,
    ): String {
        val resp = client.post("/sagas/run") {
            contentType(ContentType.Application.Json)
            setBody(asyncDefinition(defName, timeoutMillis).toJsonElement().toString())
        }
        assertEquals(200, resp.status.value, resp.bodyAsText())
        return (testJson.parseToJsonElement(resp.bodyAsText()) as kotlinx.serialization.json.JsonObject)["id"]!!.jsonPrimitive.content
    }

    // ── Test: async node pauses execution ─────────────────────────────────────

    @Test
    fun `async node pauses execution in WAITING_CALLBACK state`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-wait")

        e2eTest(wmPort = wm.port()) {
            val sagaId = startAsyncSaga(client, defName)
            // Wait until the async request reaches WireMock and executor suspends
            val finalOrWaiting = awaitSagaStatus(client, sagaId, "WAITING_CALLBACK", timeoutMs = 15_000)
            assertEquals("WAITING_CALLBACK", finalOrWaiting["status"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: valid callback resumes saga to SUCCEEDED ─────────────────────────

    @Test
    fun `valid callback resumes saga to SUCCEEDED`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-resume")

        e2eTest(wmPort = wm.port()) {
            val sagaId = startAsyncSaga(client, defName)
            awaitSagaStatus(client, sagaId, "WAITING_CALLBACK", timeoutMs = 15_000)

            // Extract callbackToken from WireMock's captured async-step request
            val callbackToken = wm.extractBodyField("/async-step/authorize", "callbackToken")

            // Fire the callback directly via the test client
            val callbackResp = client.post("/sagas/$sagaId/node/authorize/callback") {
                header("X-Callback-Token", callbackToken)
                contentType(ContentType.Application.Json)
                setBody("""{"status":"approved","amount":"100.00"}""")
            }
            assertTrue(
                callbackResp.status.value in 200..202,
                "Expected 2xx for valid callback, got ${callbackResp.status.value}: ${callbackResp.bodyAsText()}"
            )

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: wrong callback token returns 401 ────────────────────────────────

    @Test
    fun `callback with wrong token returns 401`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-bad-token")

        e2eTest(wmPort = wm.port()) {
            val sagaId = startAsyncSaga(client, defName)
            awaitSagaStatus(client, sagaId, "WAITING_CALLBACK", timeoutMs = 15_000)

            val callbackResp = client.post("/sagas/$sagaId/node/authorize/callback") {
                header("X-Callback-Token", "garbage-token-that-is-definitely-wrong")
                contentType(ContentType.Application.Json)
                setBody("""{"status":"approved"}""")
            }
            assertEquals(401, callbackResp.status.value, callbackResp.bodyAsText())
        }
    }

    // ── Test: nonce replay returns 409 ────────────────────────────────────────

    @Test
    fun `callback nonce replay returns 409 on second call`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-replay")

        e2eTest(wmPort = wm.port()) {
            val sagaId = startAsyncSaga(client, defName)
            awaitSagaStatus(client, sagaId, "WAITING_CALLBACK", timeoutMs = 15_000)

            val callbackToken = wm.extractBodyField("/async-step/authorize", "callbackToken")

            // First callback — should succeed
            val first = client.post("/sagas/$sagaId/node/authorize/callback") {
                header("X-Callback-Token", callbackToken)
                contentType(ContentType.Application.Json)
                setBody("""{"status":"approved"}""")
            }
            assertTrue(first.status.value in 200..202, "First callback should succeed, got ${first.status.value}")

            // Second callback with same token — nonce replay
            val second = client.post("/sagas/$sagaId/node/authorize/callback") {
                header("X-Callback-Token", callbackToken)
                contentType(ContentType.Application.Json)
                setBody("""{"status":"approved"}""")
            }
            assertEquals(409, second.status.value, "Second callback (replay) should return 409, got ${second.status.value}")
        }
    }

    // ── Test: wrong accepted status code triggers failure ─────────────────────

    @Test
    fun `async step receiving non-accepted status code triggers failure path`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-wrong-status")

        // Override: async-step returns 200 (not in acceptedStatusCodes: [202]) → failure
        wm.resetAll()
        wm.stubSuccessStep()
        wm.stubCallbackEndpoint()
        wm.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.post(
                com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo("/async-step/authorize")
            ).willReturn(
                com.github.tomakehurst.wiremock.client.WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"status":"ok"}""")
            )
        )

        e2eTest(wmPort = wm.port()) {
            val sagaId = startAsyncSaga(client, defName)
            val final = awaitSagaTerminal(client, sagaId, timeoutMs = 20_000)
            // Should fail since 200 is not in acceptedStatusCodes [202]
            assertTrue(
                final["status"]?.jsonPrimitive?.content in listOf("FAILED", "CORRUPTED"),
                "Expected FAILED/CORRUPTED for non-accepted status, got ${final["status"]?.jsonPrimitive?.content}"
            )
        }
    }

    // ── Test: callback timeout triggers failure ────────────────────────────────

    @Test
    fun `async callback timeout moves saga to failure`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("async-timeout")

        e2eTest(wmPort = wm.port()) {
            // Use very short timeout (800ms) so it times out quickly in tests
            val sagaId = startAsyncSaga(client, defName, timeoutMillis = 800L)

            // Wait for WAITING_CALLBACK first, then let the timeout fire
            awaitSagaStatus(client, sagaId, "WAITING_CALLBACK", timeoutMs = 15_000)

            // Don't fire callback — let the timeout expire and the executor process it
            val final = awaitSagaTerminal(client, sagaId, timeoutMs = 30_000)
            assertTrue(
                final["status"]?.jsonPrimitive?.content in listOf("FAILED", "CORRUPTED"),
                "Expected FAILED/CORRUPTED after timeout, got ${final["status"]?.jsonPrimitive?.content}"
            )
        }
    }
}
