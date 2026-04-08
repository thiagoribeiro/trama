package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
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
class E2EDefinitionApiTest {

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

    private fun v1DefinitionBody(name: String, version: String = "v1") = mapOf(
        "name" to name,
        "version" to version,
        "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 1, "delayMillis" to 10),
        "steps" to listOf(
            mapOf(
                "name" to "charge",
                "up" to mapOf(
                    "url" to svc("/step/charge?phase=up"),
                    "verb" to "POST",
                    "headers" to mapOf("Content-Type" to "application/json"),
                    "body" to mapOf("step" to "charge"),
                ),
                "down" to mapOf(
                    "url" to svc("/step/charge?phase=down"),
                    "verb" to "POST",
                    "headers" to mapOf("Content-Type" to "application/json"),
                    "body" to mapOf("compensate" to "charge"),
                ),
            ),
        ),
        "onSuccessCallback" to mapOf(
            "url" to svc("/callback/success"),
            "verb" to "POST",
            "headers" to mapOf("Content-Type" to "application/json"),
            "body" to mapOf("status" to "ok"),
        ),
    )

    // ── Test: POST definition returns 200 and is retrievable by id ────────────

    @Test
    fun `post definition returns 200 and is retrievable by id`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("def-crud")

        e2eTest(wmPort = wm.port()) {
            val resp = client.post("/sagas/definitions") {
                contentType(ContentType.Application.Json)
                setBody(v1DefinitionBody(defName).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val created = testJson.parseToJsonElement(resp.bodyAsText()).jsonObject
            val defId = created["id"]?.jsonPrimitive?.content
            assertNotNull(defId, "Expected 'id' in response")
            assertEquals(defName, created["name"]?.jsonPrimitive?.content)
            assertEquals("v1", created["version"]?.jsonPrimitive?.content)

            // Retrieve by id
            val getResp = client.get("/sagas/definitions/$defId")
            assertEquals(200, getResp.status.value, getResp.bodyAsText())
            val fetched = testJson.parseToJsonElement(getResp.bodyAsText()).jsonObject
            assertEquals(defId, fetched["id"]?.jsonPrimitive?.content)
            assertEquals(defName, fetched["name"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: duplicate name+version returns 409 ──────────────────────────────

    @Test
    fun `duplicate name version returns 409`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("def-dup")

        e2eTest(wmPort = wm.port()) {
            val body = v1DefinitionBody(defName).toJsonElement().toString()

            val first = client.post("/sagas/definitions") {
                contentType(ContentType.Application.Json)
                setBody(body)
            }
            assertEquals(200, first.status.value, first.bodyAsText())

            val second = client.post("/sagas/definitions") {
                contentType(ContentType.Application.Json)
                setBody(body)
            }
            assertEquals(409, second.status.value, "Expected 409 for duplicate definition, got ${second.status.value}: ${second.bodyAsText()}")
        }
    }

    // ── Test: run stored definition saga succeeds ─────────────────────────────

    @Test
    fun `run stored definition saga succeeds`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("def-run")
        val defVersion = "v1"

        e2eTest(wmPort = wm.port()) {
            // Store the definition
            val storeResp = client.post("/sagas/definitions") {
                contentType(ContentType.Application.Json)
                setBody(v1DefinitionBody(defName, defVersion).toJsonElement().toString())
            }
            assertEquals(200, storeResp.status.value, storeResp.bodyAsText())

            // Run it by name/version with a payload
            val runResp = client.post("/sagas/definitions/$defName/$defVersion/run") {
                contentType(ContentType.Application.Json)
                setBody("""{"payload":{"orderId":"stored-run-001"}}""")
            }
            assertEquals(200, runResp.status.value, runResp.bodyAsText())
            val sagaId = testJson.parseToJsonElement(runResp.bodyAsText()).jsonObject["id"]?.jsonPrimitive?.content
            assertNotNull(sagaId, "Expected saga id in run response")

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        }
    }

    // ── Test: GET saga returns status field ───────────────────────────────────

    @Test
    fun `get saga returns status and id fields`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("def-status")

        e2eTest(wmPort = wm.port()) {
            // Run an inline saga and poll its status
            val runResp = client.post("/sagas/run") {
                contentType(ContentType.Application.Json)
                setBody(
                    mapOf(
                        "definition" to v1DefinitionBody(defName),
                        "payload" to mapOf("orderId" to "status-test-001"),
                    ).toJsonElement().toString()
                )
            }
            assertEquals(200, runResp.status.value, runResp.bodyAsText())
            val sagaId = testJson.parseToJsonElement(runResp.bodyAsText()).jsonObject["id"]?.jsonPrimitive?.content
            assertNotNull(sagaId)

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals(sagaId, final["id"]?.jsonPrimitive?.content)
            assertNotNull(final["status"]?.jsonPrimitive?.content)
            assertNotNull(final["name"]?.jsonPrimitive?.content)
            assertNotNull(final["startedAt"]?.jsonPrimitive?.content)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        }
    }
}
