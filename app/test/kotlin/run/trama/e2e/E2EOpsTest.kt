package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Health, readiness and Prometheus metrics on a live runtime. */
class E2EOpsTest {
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

    @Test
    fun `healthz and readyz report ok on a running runtime`() = e2eTest {
        val health = client.get("/healthz")
        assertEquals(200, health.status.value)
        assertEquals("ok", health.bodyAsText())

        // Readiness depends on the first membership refresh; give it a moment.
        var ready = client.get("/readyz")
        val deadline = System.currentTimeMillis() + 10_000
        while (ready.status.value != 200 && System.currentTimeMillis() < deadline) {
            kotlinx.coroutines.delay(200)
            ready = client.get("/readyz")
        }
        assertEquals(200, ready.status.value, ready.bodyAsText())
        assertEquals("ready", ready.bodyAsText())
    }

    @Test
    fun `metrics endpoint is absent when metrics are disabled`() = e2eTest {
        assertEquals(404, client.get("/metrics").status.value)
    }

    @Test
    fun `metrics endpoint exposes saga metrics after an execution`() = e2eTest(props = mapOf("metrics.enabled" to "true")) {
        val name = uniqueName("metrics")
        val id = client.runInline(v2DefinitionMap(name, listOf(taskNodeMap("a", "http://localhost:${wm.port()}/step/a"))))
        awaitSagaTerminal(client, id)

        val scrape = client.get("/metrics")
        assertEquals(200, scrape.status.value)
        val text = scrape.bodyAsText()
        assertTrue(text.contains("saga_duration_seconds_count") && text.contains(name), "saga duration timer missing for $name")
        assertTrue(text.contains("saga_step_duration_success"), "step duration timer missing")
        assertTrue(text.lines().any { it.startsWith("saga_dequeue") }, "dequeue counter missing")
    }

    @Test
    fun `metrics endpoint exposes the enqueue counter`() = e2eTest(props = mapOf("metrics.enabled" to "true")) {
        val id = client.runInline(v2DefinitionMap(uniqueName("metrics-enq"), listOf(taskNodeMap("a", "http://localhost:${wm.port()}/step/a"))))
        awaitSagaTerminal(client, id)
        assertTrue(client.get("/metrics").bodyAsText().lines().any { it.startsWith("saga_enqueue") }, "enqueue counter missing")
    }
}
