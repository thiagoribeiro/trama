package run.trama.saga.store

import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.RESERVED_NODES_KEY
import run.trama.saga.RecordingEnqueuer
import run.trama.saga.RecordingHttp
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaExecution
import run.trama.saga.SagaRepositoryStore
import run.trama.saga.TaskMode
import run.trama.saga.httpCall
import run.trama.saga.testExecutor
import run.trama.saga.v2Execution

/**
 * A step's response body must be the same value whether a later node reads it in the slice that
 * ran the step (in memory) or after a reload from Postgres: in `nodes.<id>` and in a split
 * branch's `payload._nodes.<id>`. Runs the same graph twice against the real store, once with a
 * sleep between the step and its readers (forcing the reload), once without.
 */
class StepBodyReloadIntegrationTest {
    private lateinit var store: SagaRepositoryStore

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        store = SagaRepositoryStore(SagaRepository(IntegrationDb.client))
    }

    private val jsonHeaders = mapOf("Content-Type" to "application/json")

    /** pre → [nap →] show → fan-out(a, b) → fan-in. `show` and branch `a` echo pre's body as typed JSON. */
    private fun definition(withReload: Boolean): SagaDefinitionV2 {
        val afterPre = if (withReload) "nap" else "show"
        return SagaDefinitionV2(
            name = "step-body-${UUID.randomUUID()}",
            version = "1",
            failureHandling = FailureHandling.Retry(0, 0),
            entrypoint = "pre",
            nodes = listOfNotNull(
                NodeDefinition.Task("pre", NodeActionDef(TaskMode.SYNC, httpCall("http://pre/")), next = afterPre),
                if (withReload) NodeDefinition.Sleep("nap", durationMillis = 1, next = "show") else null,
                NodeDefinition.Task(
                    "show",
                    NodeActionDef(TaskMode.SYNC, httpCall("http://show/", body = """{"v":"{{nodes.pre.response.body}}"}""", headers = jsonHeaders)),
                    next = "fan-out",
                ),
                NodeDefinition.Split("fan-out", branches = listOf("a", "b"), join = "fan-in"),
                NodeDefinition.Task("a", NodeActionDef(TaskMode.SYNC, httpCall("http://a/", body = """{"v":"{{payload._nodes.pre}}"}""", headers = jsonHeaders))),
                NodeDefinition.Task("b", NodeActionDef(TaskMode.SYNC, httpCall("http://b/"))),
                NodeDefinition.Join("fan-in"),
            ),
        )
    }

    private data class Observed(val showBody: JsonElement, val nodes: JsonObject)

    private fun run(preBody: String, withReload: Boolean): Observed = runBlocking {
        val http = RecordingHttp { req ->
            if (req.url.host == "pre") respond(preBody, HttpStatusCode.OK, headersOf("Content-Type", "text/plain"))
            else respond("{}", HttpStatusCode.OK, headersOf("Content-Type", "application/json"))
        }
        val enqueuer = RecordingEnqueuer()
        val executor = testExecutor(store, enqueuer, http.provider)
        val parent = v2Execution(definition(withReload))

        executor.execute(parent)
        if (withReload) {
            val sleeping = enqueuer.enqueued.last { it.execution.id == parent.id }.execution
            check(sleeping.state is ExecutionState.Sleeping) { "expected the parent to sleep, got ${sleeping.state}" }
            Thread.sleep(5)
            executor.execute(sleeping)
        }

        val show = assertNotNull(http.requests.singleOrNull { it.url.host == "show" }, "show must run once")
        val showBody = Json.parseToJsonElement(String(show.body.toByteArray()))
        val child: SagaExecution = enqueuer.enqueued.map { it.execution }.single { it.branchId == "a" }
        Observed(showBody, child.payload.getValue(RESERVED_NODES_KEY).value as JsonObject)
    }

    private fun assertSameAcrossReload(preBody: String, expected: JsonElement) {
        val inMemory = run(preBody, withReload = false)
        val reloaded = run(preBody, withReload = true)
        assertEquals(JsonObject(mapOf("v" to expected)), inMemory.showBody, "nodes.pre in memory")
        assertEquals(inMemory.showBody, reloaded.showBody, "nodes.pre after a reload")
        assertEquals(JsonObject(mapOf("pre" to expected)), inMemory.nodes, "_nodes in memory")
        assertEquals(inMemory.nodes, reloaded.nodes, "_nodes after a reload")
    }

    @Test
    fun `an empty body is null before and after a reload`() = assertSameAcrossReload("", JsonNull)

    @Test
    fun `a plain-text body is a JSON string before and after a reload`() =
        assertSameAcrossReload("approved by ops", JsonPrimitive("approved by ops"))

    @Test
    fun `a bare word is a JSON string before and after a reload`() = assertSameAcrossReload("ok", JsonPrimitive("ok"))

    @Test
    fun `a JSON body is that JSON before and after a reload`() =
        assertSameAcrossReload("""{"z":1,"a":{"list":[true,null,"s"]}}""", Json.parseToJsonElement("""{"z":1,"a":{"list":[true,null,"s"]}}"""))
}
