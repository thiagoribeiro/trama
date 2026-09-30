package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class E2ESleepSagaTest {
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

    private fun sleepDef(name: String, sleepMillis: Long, terminal: Boolean = false) = v2DefinitionMap(
        name,
        if (terminal) {
            listOf(taskNodeMap("a", svc("/step/a"), next = "nap"), sleepNodeMap("nap", sleepMillis))
        } else {
            listOf(
                taskNodeMap("a", svc("/step/a"), next = "nap"),
                sleepNodeMap("nap", sleepMillis, next = "b"),
                taskNodeMap("b", svc("/step/b")),
            )
        },
    )

    /** Waits until node `a` has been called, i.e. the saga has reached the sleep node. */
    private suspend fun awaitFirstNode() {
        val deadline = System.currentTimeMillis() + 10_000
        while (wm.requestsTo("/step/a").isEmpty()) {
            check(System.currentTimeMillis() < deadline) { "node a was never called" }
            kotlinx.coroutines.delay(50)
        }
        kotlinx.coroutines.delay(300) // let the executor persist the Sleeping state
    }

    @Test
    fun `sleep pauses between nodes and then completes`() = e2eTest {
        val id = client.runInline(sleepDef(uniqueName("sleep"), sleepMillis = 1_500))

        awaitFirstNode()
        assertEquals(0, wm.requestsTo("/step/b").size, "b must not run while sleeping")

        val final = awaitSagaTerminal(client, id, timeoutMs = 20_000)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        val aAt = wm.requestsTo("/step/a").single().loggedDate.time
        val bAt = wm.requestsTo("/step/b").single().loggedDate.time
        assertTrue(bAt - aAt >= 1_400, "b ran only ${bAt - aAt}ms after a; expected >= 1500ms sleep")
    }

    @Disabled(
        "BUG: RedisSagaExecutionStore.saveSleeping calls repository.updateStatus(SLEEPING) without first " +
            "upserting the saga_execution row (saveWaiting/saveWaitingJoin do upsert). Under the default REDIS " +
            "store that row only exists after finalization, so the UPDATE matches nothing and " +
            "GET /workflows/{id} returns 204 for the whole sleep instead of SLEEPING.",
    )
    @Test
    fun `status API reports SLEEPING while the saga sleeps`() = e2eTest {
        val id = client.runInline(sleepDef(uniqueName("sleep-status"), sleepMillis = 10 * 60_000))
        awaitSagaStatus(client, id, "SLEEPING", timeoutMs = 10_000)
    }

    @Disabled(
        "BUG: same root cause as the SLEEPING status bug. wakeExecution looks the saga up in Postgres first, finds " +
            "no row, and answers 404, so POST /workflows/{id}/wake never works under the default REDIS store.",
    )
    @Test
    fun `wake endpoint cuts a long sleep short`() = e2eTest {
        val id = client.runInline(sleepDef(uniqueName("wake"), sleepMillis = 10 * 60_000))
        awaitFirstNode()

        val wake = client.postJson("/workflows/$id/wake", null)
        assertEquals(202, wake.status.value)

        val final = awaitSagaTerminal(client, id, timeoutMs = 15_000)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/b").size)

        assertEquals(409, client.postJson("/workflows/$id/wake", null).status.value, "a finished saga is not sleeping")
    }

    @Test
    fun `wake validates the execution id`() = e2eTest {
        assertEquals(400, client.postJson("/workflows/not-a-uuid/wake", null).status.value)
        assertEquals(404, client.postJson("/workflows/${UUID.randomUUID()}/wake", null).status.value)
    }

    @Test
    fun `sleep as the terminal node finishes the saga on its own`() = e2eTest {
        val id = client.runInline(sleepDef(uniqueName("sleep-last"), sleepMillis = 500, terminal = true))
        val final = awaitSagaTerminal(client, id, timeoutMs = 15_000)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
    }

    @Disabled(
        "BUG (masked today by the 404 above): RuntimeBootstrap.wakeExecution builds " +
            "InProgress(activeNodeId = Sleeping.nextNodeId), which is null when the sleep is the terminal node. " +
            "WorkflowExecutor then takes the legacy v1 path (resolveActiveNodeId → definition.steps), which is " +
            "empty for v2 → coerceIn(0, -1) throws and the execution never finishes.",
    )
    @Test
    fun `waking a terminal sleep finishes the saga`() = e2eTest {
        val id = client.runInline(sleepDef(uniqueName("wake-last"), sleepMillis = 10 * 60_000, terminal = true))
        awaitFirstNode()

        assertEquals(202, client.postJson("/workflows/$id/wake", null).status.value)

        val final = awaitSagaTerminal(client, id, timeoutMs = 15_000)
        assertEquals("SUCCEEDED", final["status"]?.jsonPrimitive?.content)
    }
}
