package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.statement.bodyAsText
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class E2ERetryEndpointTest {
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

    private fun v1Def(name: String) = mapOf(
        "name" to name,
        "version" to "v1",
        "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 0, "delayMillis" to 10),
        "steps" to listOf("reserve", "charge").map { step ->
            mapOf(
                "name" to step,
                "up" to httpCallMap(svc("/step/$step"), mapOf("order" to "{{payload.orderId}}")),
                "down" to httpCallMap(svc("/undo/$step")),
            )
        },
    )

    /** Runs the v1 saga with charge failing, and returns its id once it is FAILED. */
    private suspend fun io.ktor.client.HttpClient.failedSaga(name: String): String {
        wm.stubPath("/step/charge", 500)
        wm.stubPath("/undo/reserve", 200)
        val id = runInline(v1Def(name), mapOf("orderId" to "ord-7"))
        val final = awaitSagaTerminal(this, id)
        assertEquals("FAILED", final["status"]?.jsonPrimitive?.content)
        return id
    }

    @Test
    fun `retrying a FAILED v1 saga re-runs it from the start and can succeed`() = e2eTest {
        val id = client.failedSaga(uniqueName("retry"))
        wm.stubPath("/step/charge", 200)

        val resp = client.postJson("/workflows/$id/retry", null)
        assertEquals(202, resp.status.value)
        assertEquals("REQUEUED", testJson.parseToJsonElement(resp.bodyAsTextSafe()).jsonObject["status"]?.jsonPrimitive?.content)

        val final = awaitSagaTerminal(client, id)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        // failedStepIndex is never persisted by the executor, so retry restarts at step 0. Since the
        // failed run already compensated reserve, re-running it is the correct behavior.
        assertEquals(2, wm.requestsTo("/step/reserve").size)
        assertEquals(2, wm.requestsTo("/step/charge").size)
    }

    @Disabled(
        "BUG: POST /workflows/{id}/retry rebuilds the execution with payload = emptyMap(), since the payload is " +
            "never persisted. Every {{payload.*}} template renders empty on the retried run.",
    )
    @Test
    fun `retry keeps the original payload`() = e2eTest {
        val id = client.failedSaga(uniqueName("retry-payload"))
        wm.stubPath("/step/charge", 200)
        assertEquals(202, client.postJson("/workflows/$id/retry", null).status.value)
        awaitSagaTerminal(client, id)

        val retriedCharge = wm.requestsTo("/step/charge").last()
        val order = testJson.parseToJsonElement(retriedCharge.bodyAsString).jsonObject["order"]?.jsonPrimitive?.content
        assertEquals("ord-7", order)
    }

    @Disabled(
        "BUG: POST /workflows/{id}/retry does not check the current status. A SUCCEEDED (or still IN_PROGRESS) " +
            "saga is re-queued and every step runs again, including non-idempotent side effects.",
    )
    @Test
    fun `retry refuses sagas that did not fail`() = e2eTest {
        wm.stubPath("/undo/reserve", 200)
        val id = client.runInline(v1Def(uniqueName("retry-ok")), mapOf("orderId" to "o"))
        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)

        val resp = client.postJson("/workflows/$id/retry", null)
        assertEquals(409, resp.status.value)
    }

    @Disabled(
        "BUG: v2 retry is documented as 422, but returns 500. saga_execution.definition stores the v1 *stub* " +
            "(execution.definition, steps = []), not the v2 graph, so the containsKey(\"nodes\") check never matches " +
            "and definition.steps.last() throws NoSuchElementException.",
    )
    @Test
    fun `retry of a v2 saga is rejected as unsupported`() = e2eTest {
        wm.stubPath("/step/b", 500)
        val id = client.runInline(v2DefinitionMap(uniqueName("retry-v2"), listOf(taskNodeMap("b", svc("/step/b")))))
        assertEquals("FAILED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)

        assertEquals(422, client.postJson("/workflows/$id/retry", null).status.value)
    }

    @Test
    fun `retry validates the execution id`() = e2eTest {
        assertEquals(400, client.postJson("/workflows/nope/retry", null).status.value)
        assertEquals(204, client.postJson("/workflows/${UUID.randomUUID()}/retry", null).status.value)
    }
}

private suspend fun io.ktor.client.statement.HttpResponse.bodyAsTextSafe(): String =
    bodyAsText().also { assertTrue(it.isNotBlank(), "empty body") }
