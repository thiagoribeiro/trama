package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** GET /workflows (list/search), /workflows/{id}, /workflows/{id}/steps and /steps/calls. */
class E2EExecutionQueryTest {
    private lateinit var wm: WireMockServer

    @BeforeEach
    fun setUp() {
        assumeDocker()
        wm = WireMockServer(wireMockConfig().dynamicPort()).also { it.start() }
        wm.stubSuccessStep()
        wm.stubPath("/undo/a", 200)
    }

    @AfterEach
    fun tearDown() {
        if (::wm.isInitialized) wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    // Distinct versions per variant: the normalizer caches graphs by name:version (see the
    // @Disabled cache test below), so reusing name+version with different content is unsafe.
    private fun def(name: String, failing: Boolean, maxAttempts: Int = 0) = v2DefinitionMap(
        name,
        listOf(
            taskNodeMap("a", svc("/step/a/{{payload.orderId}}"), next = "b", compensationUrl = svc("/undo/a"), body = mapOf("order" to "{{payload.orderId}}")),
            taskNodeMap("b", svc(if (failing) "/fail/b" else "/step/b")),
        ),
        failureHandling = mapOf("type" to "retry", "maxAttempts" to maxAttempts, "delayMillis" to 10),
        version = if (failing) "fail-$maxAttempts" else "ok-$maxAttempts",
    )

    @org.junit.jupiter.api.Disabled(
        "BUG: DefinitionNormalizer caches the normalized graph by name:version for the life of the JVM. A second " +
            "inline POST /workflows/run with the same name/version but a different graph executes the first one.",
    )
    @Test
    fun `inline runs with the same name and version execute their own graph`() = e2eTest {
        wm.stubPath("/fail/b", 500)
        val name = uniqueName("same-nv")
        val ok = v2DefinitionMap(name, listOf(taskNodeMap("b", svc("/step/b"))))
        val failing = v2DefinitionMap(name, listOf(taskNodeMap("b", svc("/fail/b"))))

        awaitSagaTerminal(client, client.runInline(ok))
        val final = awaitSagaTerminal(client, client.runInline(failing))

        assertEquals("FAILED", final["status"]?.jsonPrimitive?.content, "the second graph (failing /fail/b) was not executed")
    }

    @Test
    fun `list filters by name and status and paginates newest first`() = e2eTest {
        wm.stubPath("/fail/b", 500)
        val name = uniqueName("query")
        val ok1 = client.runInline(def(name, failing = false))
        awaitSagaTerminal(client, ok1)
        val failed = client.runInline(def(name, failing = true))
        awaitSagaTerminal(client, failed)
        val ok2 = client.runInline(def(name, failing = false))
        awaitSagaTerminal(client, ok2)

        fun ids(body: kotlinx.serialization.json.JsonElement?) = (body as JsonArray).map { it.jsonObject["id"]!!.jsonPrimitive.content }

        val (status, all) = client.getJson("/workflows?name=$name")
        assertEquals(200, status)
        assertEquals(listOf(ok2, failed, ok1), ids(all))

        assertEquals(listOf(failed), ids(client.getJson("/workflows?name=$name&status=FAILED").second), "status filter; full list: $all")
        assertEquals(listOf(failed), ids(client.getJson("/workflows?name=$name&limit=1&offset=1").second), "pagination")
        assertEquals(1, ids(client.getJson("/workflows?name=$name&limit=0").second).size, "limit is clamped to >= 1")
        assertEquals(3, ids(client.getJson("/workflows?name=$name&offset=-5").second).size, "negative offset is clamped to 0")

        val failedSummary = (all as JsonArray).first { it.jsonObject["id"]!!.jsonPrimitive.content == failed }.jsonObject
        assertEquals("FAILED", failedSummary["status"]!!.jsonPrimitive.content)
        assertTrue(failedSummary["completedAt"]!!.jsonPrimitive.content.isNotBlank())
    }

    private fun stepKeys(steps: List<kotlinx.serialization.json.JsonObject>) =
        steps.map { "${it["stepName"]!!.jsonPrimitive.content}:${it["phase"]!!.jsonPrimitive.content}:${it["success"]!!.jsonPrimitive.content}" }

    @Test
    fun `steps endpoint shows forward and compensation phases`() = e2eTest {
        wm.stubPath("/fail/b", 500, """{"error":"declined"}""")
        val id = client.runInline(def(uniqueName("steps"), failing = true), mapOf("orderId" to "o-1"))
        awaitSagaTerminal(client, id)

        val (status, body) = client.getJson("/workflows/$id/steps")
        assertEquals(200, status)
        val steps = body!!.jsonArray.map { it.jsonObject }
        assertEquals(setOf("a:UP:true", "b:UP:false", "a:DOWN:true"), stepKeys(steps).toSet())
        val bUp = steps.single { it["stepName"]!!.jsonPrimitive.content == "b" }
        assertEquals(500, bUp["statusCode"]!!.jsonPrimitive.int)
        assertEquals("declined", bUp["responseBody"]!!.jsonObject["error"]!!.jsonPrimitive.content)
        assertTrue(steps.all { it["latencyMs"]!!.jsonPrimitive.long >= 0 })
    }

    @org.junit.jupiter.api.Disabled(
        "BUG: under the REDIS store, step results are flushed at finalization by SagaRepository.insertStepResults " +
            "in one multi-row INSERT that stamps every row with the same created_at. getStepResults orders by " +
            "created_at, so the timeline comes back in arbitrary order (observed reversed).",
    )
    @Test
    fun `steps endpoint returns steps in execution order`() = e2eTest {
        wm.stubPath("/fail/b", 500)
        val id = client.runInline(def(uniqueName("steps-order"), failing = true), mapOf("orderId" to "o-1"))
        awaitSagaTerminal(client, id)
        val steps = client.getJson("/workflows/$id/steps").second!!.jsonArray.map { it.jsonObject }
        assertEquals(listOf("a:UP:true", "b:UP:false", "a:DOWN:true"), stepKeys(steps))
    }

    @org.junit.jupiter.api.Disabled(
        "BUG: /workflows/{id}/steps computes latencyMs = created_at - step_started_at. With the REDIS store every " +
            "created_at is the finalization time, so a step's latency includes all later steps and sleeps.",
    )
    @Test
    fun `step latency reflects the step itself, not the rest of the saga`() = e2eTest {
        val def = v2DefinitionMap(
            uniqueName("latency"),
            listOf(taskNodeMap("a", svc("/step/a"), next = "nap"), sleepNodeMap("nap", 1_500, next = "b"), taskNodeMap("b", svc("/step/b"))),
        )
        val id = client.runInline(def)
        awaitSagaTerminal(client, id, timeoutMs = 20_000)
        val a = client.getJson("/workflows/$id/steps").second!!.jsonArray.map { it.jsonObject }
            .single { it["stepName"]!!.jsonPrimitive.content == "a" }
        val latency = a["latencyMs"]!!.jsonPrimitive.long
        assertTrue(latency < 1_000, "step a took $latency ms according to the API, but its HTTP call was instant")
    }

    @Test
    fun `step calls endpoint records rendered requests and one entry per attempt`() = e2eTest {
        wm.stubPath("/fail/b", 500)
        val id = client.runInline(def(uniqueName("calls"), failing = true, maxAttempts = 1), mapOf("orderId" to "o-9"))
        awaitSagaTerminal(client, id)

        val calls = client.getJson("/workflows/$id/steps/calls").second!!.jsonArray.map { it.jsonObject }
        val a = calls.first { it["stepName"]!!.jsonPrimitive.content == "a" && it["phase"]!!.jsonPrimitive.content == "UP" }
        assertEquals(svc("/step/a/o-9"), a["requestUrl"]!!.jsonPrimitive.content)
        assertEquals("o-9", a["requestBody"]!!.jsonObject["order"]!!.jsonPrimitive.content)
        assertEquals(200, a["statusCode"]!!.jsonPrimitive.int)

        val bAttempts = calls.filter { it["stepName"]!!.jsonPrimitive.content == "b" }.map { it["attempt"]!!.jsonPrimitive.int }
        assertEquals(listOf(0, 1), bAttempts, "one call record per attempt (initial + 1 retry)")
        assertTrue(calls.any { it["stepName"]!!.jsonPrimitive.content == "a" && it["phase"]!!.jsonPrimitive.content == "DOWN" })
    }

    @Test
    fun `query endpoints validate ids and handle unknown executions`() = e2eTest {
        val unknown = UUID.randomUUID()
        assertEquals(400, client.getJson("/workflows/nope").first)
        assertEquals(400, client.getJson("/workflows/nope/steps").first)
        assertEquals(400, client.getJson("/workflows/nope/steps/calls").first)
        assertEquals(204, client.getJson("/workflows/$unknown").first)
        assertEquals(0, client.getJson("/workflows/$unknown/steps").second!!.jsonArray.size)
        assertEquals(0, client.getJson("/workflows/$unknown/steps/calls").second!!.jsonArray.size)
    }
}
