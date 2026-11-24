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
import kotlin.test.assertTrue

/**
 * End-to-end coverage for split/join over the real HTTP API + Postgres + Redis stack.
 * Requires Docker (Testcontainers) — each test no-ops if Docker is unavailable, matching
 * the pattern used by every other E2E*SagaTest in this package.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class E2ESplitJoinSagaTest {

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
    }

    @AfterEach
    fun stopWiremock() {
        wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    private fun taskNode(id: String, next: String? = null): Map<String, Any?> {
        val node: MutableMap<String, Any?> = mutableMapOf(
            "kind" to "task",
            "id" to id,
            "action" to mapOf(
                "mode" to "sync",
                "request" to mapOf(
                    "url" to svc("/step/$id"),
                    "verb" to "POST",
                ),
            ),
        )
        if (next != null) node["next"] = next
        return node
    }

    /** fan-out into two independent branches, joined back into a single "after-join" task. */
    private fun splitJoinDefinition(name: String): Map<*, *> = mapOf(
        "definition" to mapOf(
            "name" to name,
            "version" to "v1",
            "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 0, "delayMillis" to 10),
            "entrypoint" to "fan-out",
            "nodes" to listOf(
                mapOf(
                    "kind" to "split",
                    "id" to "fan-out",
                    "branches" to listOf("branch-a", "branch-b"),
                    "join" to "fan-in",
                ),
                taskNode("branch-a"),
                taskNode("branch-b"),
                mapOf("kind" to "join", "id" to "fan-in", "next" to "after-join"),
                taskNode("after-join"),
            ),
        ),
        "payload" to mapOf("orderId" to "ord-001"),
    )

    @Test
    fun `split fans out and join resumes after both branches succeed`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("split-join-success")

        e2eTest(wmPort = wm.port()) {
            val resp = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody(splitJoinDefinition(defName).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val sagaId = (testJson.parseToJsonElement(resp.bodyAsText()).jsonObject)["id"]!!.jsonPrimitive.content

            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            val upRequests = wm.findAll(postRequestedFor(urlMatching("/step/.*"))).map { it.url }
            assertTrue(upRequests.any { it.contains("branch-a") }, "expected branch-a to run")
            assertTrue(upRequests.any { it.contains("branch-b") }, "expected branch-b to run")
            assertTrue(upRequests.any { it.contains("after-join") }, "expected after-join to run once the join fired")
        }
    }

    @Test
    fun `join surfaces a partial branch failure without blocking the barrier`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        val defName = uniqueName("split-join-partial-failure")
        wm.stubFor(
            com.github.tomakehurst.wiremock.client.WireMock.post(com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo("/step/branch-b"))
                .willReturn(com.github.tomakehurst.wiremock.client.WireMock.aResponse().withStatus(500))
        )

        e2eTest(wmPort = wm.port()) {
            val resp = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody(splitJoinDefinition(defName).toJsonElement().toString())
            }
            assertEquals(200, resp.status.value, resp.bodyAsText())
            val sagaId = (testJson.parseToJsonElement(resp.bodyAsText()).jsonObject)["id"]!!.jsonPrimitive.content

            // branch-b fails and is not retried (maxAttempts=0); branch-a still succeeds.
            // The parent still advances past the join with the failure surfaced in the
            // join's own step result — there is no automatic cross-branch rollback.
            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            val upRequests = wm.findAll(postRequestedFor(urlMatching("/step/.*"))).map { it.url }
            assertTrue(upRequests.any { it.contains("after-join") }, "expected after-join to run despite branch-b failing")
        }
    }

    @Test
    fun `branches read the parent's earlier node results through payload_nodes`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        wm.stubPath("/step/load", 200, """{"customer":"c-42","secret":"s"}""")
        val definition = v2DefinitionMap(
            uniqueName("split-join-nodes"),
            listOf(
                taskNodeMap("load", svc("/step/load"), next = "nap"),
                // Resuming after the sleep reloads the parent's results from Postgres before the split.
                sleepNodeMap("nap", durationMillis = 10, next = "fan-out"),
                mapOf("kind" to "split", "id" to "fan-out", "branches" to listOf("a", "b"), "join" to "fan-in"),
                taskNodeMap("a", svc("/step/a/{{payload._nodes.load.customer}}"), body = mapOf("order" to "{{payload.orderId}}", "customer" to "{{payload._nodes.load.customer}}")),
                taskNodeMap("b", svc("/step/b"), next = "inner"),
                mapOf("kind" to "split", "id" to "inner", "branches" to listOf("leaf", "leaf2"), "join" to "inner-join"),
                taskNodeMap("leaf", svc("/step/leaf"), body = mapOf("customer" to "{{payload._nodes.load.customer}}", "fromB" to "{{payload._nodes.b.status}}")),
                taskNodeMap("leaf2", svc("/step/leaf2")),
                mapOf("kind" to "join", "id" to "inner-join"),
                mapOf("kind" to "join", "id" to "fan-in", "next" to "after"),
                taskNodeMap("after", svc("/step/after")),
            ),
        )

        e2eTest(wmPort = wm.port()) {
            val sagaId = client.runInline(definition, mapOf("orderId" to "ord-7"))
            val final = awaitSagaTerminal(client, sagaId)
            assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)

            assertEquals(1, wm.requestsTo("/step/a/c-42").size, "branch a's URL must resolve payload._nodes.load.customer")
            assertEquals("c-42", wm.extractBodyField("/step/a/c-42", "customer"))
            assertEquals("ord-7", wm.extractBodyField("/step/a/c-42", "order"))
            assertEquals("c-42", wm.extractBodyField("/step/leaf", "customer"), "a nested branch sees the grandparent's nodes")
            assertEquals("ok", wm.extractBodyField("/step/leaf", "fromB"), "and its own parent's nodes")
            assertEquals(1, wm.requestsTo("/step/after").size)
        }
    }

    @Test
    fun `starting a saga with the reserved _nodes key in its payload is rejected`() {
        if (!DockerClientFactory.instance().isDockerAvailable) return
        e2eTest(wmPort = wm.port()) {
            val resp = client.postJson(
                "/workflows/run",
                mapOf(
                    "definition" to v2DefinitionMap(uniqueName("reserved"), listOf(taskNodeMap("a", svc("/step/a")))),
                    "payload" to mapOf("_nodes" to mapOf("x" to 1)),
                ),
            )
            assertEquals(400, resp.status.value, resp.bodyAsText())
            assertTrue(wm.requestsTo("/step/a").isEmpty())
        }
    }
}
