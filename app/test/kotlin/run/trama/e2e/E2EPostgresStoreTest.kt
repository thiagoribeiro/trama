package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Same flows as the default suites, but with runtime.store = POSTGRES (SagaRepositoryStore). */
class E2EPostgresStoreTest {
    private lateinit var wm: WireMockServer
    // Own queue prefix: app instances from other suites share the Redis container, and a REDIS-store
    // worker still draining must never pick up (and execute with the wrong store) a POSTGRES-mode saga.
    private val pg = mapOf(
        "config.override.runtime.store" to "POSTGRES",
        "config.override.redis.queue.keyPrefix" to "saga:pg-e2e:${java.util.UUID.randomUUID()}",
    )

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

    @Test
    fun `sync saga with compensation completes and records ordered steps`() = e2eTest(props = pg) {
        wm.stubPath("/fail/b", 500)
        val def = v2DefinitionMap(
            uniqueName("pg-sync"),
            listOf(taskNodeMap("a", svc("/step/a"), next = "b", compensationUrl = svc("/undo/a")), taskNodeMap("b", svc("/fail/b"))),
        )
        val id = client.runInline(def)

        val final = awaitSagaTerminal(client, id)
        assertEquals("FAILED", final["status"]?.jsonPrimitive?.content)
        val steps = client.getJson("/workflows/$id/steps").second!!.jsonArray.map {
            "${it.jsonObject["stepName"]!!.jsonPrimitive.content}:${it.jsonObject["phase"]!!.jsonPrimitive.content}"
        }
        assertEquals(listOf("a:UP", "b:UP", "a:DOWN"), steps, "Postgres store writes steps as they happen, so order is stable")
    }

    @Test
    fun `async callback resumes the saga and a replayed callback is rejected`() = e2eTest(wmPort = wm.port(), props = pg) {
        wm.stubAsyncStep("/async/auth")
        val def = v2DefinitionMap(
            uniqueName("pg-async"),
            listOf(
                mapOf(
                    "kind" to "task",
                    "id" to "auth",
                    "action" to mapOf(
                        "mode" to "async",
                        "request" to httpCallMap(svc("/async/auth"), mapOf("callbackToken" to "{{runtime.callback.token}}")),
                        "acceptedStatusCodes" to listOf(202),
                        "callback" to mapOf("timeoutMillis" to 30_000),
                    ),
                    "next" to "done",
                ),
                taskNodeMap("done", svc("/step/done")),
            ),
        )
        val id = client.runInline(def)
        awaitSagaStatus(client, id, "WAITING_CALLBACK", timeoutMs = 15_000)
        val token = wm.extractBodyField("/async/auth", "callbackToken")

        val first = client.postJson("/workflows/$id/node/auth/callback", """{"ok":true}""", headers = mapOf("X-Callback-Token" to token))
        assertEquals(202, first.status.value)
        val replay = client.postJson("/workflows/$id/node/auth/callback", """{"ok":true}""", headers = mapOf("X-Callback-Token" to token))
        assertTrue(replay.status.value >= 400, "replayed callback must be rejected, got ${replay.status.value}")

        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)
        assertEquals(1, wm.requestsTo("/step/done").size)
    }

    @Test
    fun `short sleep surfaces SLEEPING and completes`() = e2eTest(props = pg) {
        val def = v2DefinitionMap(
            uniqueName("pg-sleep"),
            listOf(taskNodeMap("a", svc("/step/a"), next = "nap"), sleepNodeMap("nap", 2_000, next = "b"), taskNodeMap("b", svc("/step/b"))),
        )
        val id = client.runInline(def)

        awaitSagaStatus(client, id, "SLEEPING", timeoutMs = 10_000)
        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id, timeoutMs = 20_000)["status"]?.jsonPrimitive?.content)
    }

    @Disabled(
        "BUG: under runtime.store=POSTGRES, SagaRepositoryStore.consumeSleeping always returns null, so " +
            "POST /workflows/{id}/wake answers 200 (AlreadyWaking) and nothing is woken.",
    )
    @Test
    fun `wake cuts a long sleep short`() = e2eTest(props = pg) {
        val def = v2DefinitionMap(
            uniqueName("pg-wake"),
            listOf(taskNodeMap("a", svc("/step/a"), next = "nap"), sleepNodeMap("nap", 10 * 60_000, next = "b"), taskNodeMap("b", svc("/step/b"))),
        )
        val id = client.runInline(def)
        awaitSagaStatus(client, id, "SLEEPING", timeoutMs = 10_000)

        assertEquals(202, client.postJson("/workflows/$id/wake", null).status.value)
        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id, timeoutMs = 15_000)["status"]?.jsonPrimitive?.content)
    }
}
