package run.trama.saga.workflow

import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpStatusCode
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.ExecutionOutcome
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.NodeDefinition
import run.trama.saga.PayloadValue
import run.trama.saga.RecordingEnqueuer
import run.trama.saga.RecordingHttp
import run.trama.saga.RecordingStore
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.httpCall
import run.trama.saga.syncTask
import run.trama.saga.testExecutor
import run.trama.saga.v2Execution

/**
 * Executor behaviors not covered by DefaultSagaExecutorTest / SplitJoinWorkflowExecutorTest:
 * sleep nodes, success/failure hooks, and compensation edge cases.
 */
class WorkflowExecutorBehaviorTest {

    /** Responds 500 for any URL containing "/fail", 200 otherwise. */
    private fun http(failing: (String) -> Boolean = { it.contains("/fail") }) = RecordingHttp { req ->
        if (failing(req.url.toString())) respond("""{"error":"x"}""", HttpStatusCode.InternalServerError)
        else respond("""{"ok":true}""", HttpStatusCode.OK)
    }

    private fun RecordingHttp.urls() = requests.map { it.url.encodedPath }

    private fun def(
        vararg nodes: NodeDefinition,
        entrypoint: String = nodes.first().id,
        failureHandling: FailureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
        onSuccess: run.trama.saga.HttpCall? = null,
        onFailure: run.trama.saga.HttpCall? = null,
    ) = SagaDefinitionV2("behav-${java.util.UUID.randomUUID()}", "1", failureHandling, entrypoint, nodes.toList(), onSuccess, onFailure)

    // ── Sleep ────────────────────────────────────────────────────────────────

    @Test
    fun `sleep node parks the execution and enqueues the first chunk`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a", next = "nap"),
            NodeDefinition.Sleep("nap", durationMillis = 5_000, next = "b"),
            syncTask("b", "http://svc/b"),
        )
        val exec = v2Execution(d)
        val executor = testExecutor(store, enqueuer, http.provider, sleepMaxChunkMillis = 1_000, sleepJitterMillis = 0)

        val outcome = executor.execute(exec)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertEquals(listOf("/a"), http.urls(), "b must not run before waking")
        val entry = assertNotNull(store.sleeping[exec.id])
        val state = assertIs<ExecutionState.Sleeping>(entry.execution.state)
        assertEquals("b", state.nextNodeId)
        assertEquals(listOf("a"), state.completedNodes)
        val (queued, delay) = enqueuer.enqueued.single()
        assertIs<ExecutionState.Sleeping>(queued.state)
        assertEquals(1_000, delay, "delay is capped at one chunk")
        assertNull(store.finalStatus)
    }

    @Test
    fun `short sleep enqueues exactly its duration`() = runBlocking<Unit> {
        val enqueuer = RecordingEnqueuer()
        val d = def(NodeDefinition.Sleep("nap", durationMillis = 300, next = null))
        testExecutor(RecordingStore(), enqueuer, http().provider, sleepMaxChunkMillis = 1_000, sleepJitterMillis = 0)
            .execute(v2Execution(d))
        assertEquals(300, enqueuer.enqueued.single().delayMillis)
    }

    @Test
    fun `early sleeping delivery re-enqueues when sentinel still exists`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val d = def(NodeDefinition.Sleep("nap", 60_000, next = null))
        val exec = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().plusSeconds(60), null, emptyList(), emptyList()))
        store.saveSleeping(exec, (exec.state as ExecutionState.Sleeping).wakeAt)

        val outcome = testExecutor(store, enqueuer, http().provider, sleepMaxChunkMillis = 10_000, sleepJitterMillis = 0).execute(exec)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertEquals(10_000, enqueuer.enqueued.single().delayMillis)
        assertNotNull(store.sleeping[exec.id], "sentinel must survive an early delivery")
        assertNull(store.finalStatus)
    }

    @Test
    fun `early sleeping delivery without sentinel is discarded as stale`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val d = def(NodeDefinition.Sleep("nap", 60_000, next = null))
        val exec = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().plusSeconds(60), null, emptyList(), emptyList()))

        val outcome = testExecutor(store, enqueuer, http().provider).execute(exec)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertTrue(enqueuer.enqueued.isEmpty(), "stale item must be dropped, not re-enqueued")
        assertNull(store.finalStatus)
    }

    @Test
    fun `waking after wakeAt continues at next node and consumes the sentinel`() = runBlocking<Unit> {
        val store = RecordingStore()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a", next = "nap"),
            NodeDefinition.Sleep("nap", 1, next = "b"),
            syncTask("b", "http://svc/b"),
        )
        val exec = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().minusMillis(1), "b", listOf("a"), emptyList()))
        store.saveSleeping(exec, Instant.now())

        val outcome = testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec)

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals(listOf("/b"), http.urls())
        assertEquals("SUCCEEDED", store.finalStatus)
        assertNull(store.sleeping[exec.id])
        assertIs<ExecutionState.InProgress>(store.checkpoints.first().state, "status goes back to IN_PROGRESS on wake")
    }

    @Test
    fun `terminal sleep finishes the saga on wake`() = runBlocking<Unit> {
        val store = RecordingStore()
        val d = def(NodeDefinition.Sleep("nap", 1, next = null))
        val exec = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().minusMillis(1), null, emptyList(), emptyList()))
        store.saveSleeping(exec, Instant.now())

        val outcome = testExecutor(store, RecordingEnqueuer(), http().provider).execute(exec)

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals("SUCCEEDED", store.finalStatus)
    }

    @Test
    fun `a sleep item past wakeAt whose sentinel was already consumed is discarded`() = runBlocking<Unit> {
        // e.g. the original chunk arriving after /wake already advanced the saga.
        val store = RecordingStore()
        val http = http()
        val d = def(NodeDefinition.Sleep("nap", 1, next = "b"), syncTask("b", "http://svc/b"))
        val stale = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().minusMillis(1), "b", emptyList(), emptyList()))

        val outcome = testExecutor(store, RecordingEnqueuer(), http.provider).execute(stale)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertTrue(http.requests.isEmpty(), "the next node must not run a second time")
        assertNull(store.finalStatus)
    }

    @Test
    fun `a woken terminal sleep (re-delivered with wakeAt = now) finishes the v2 saga`() = runBlocking<Unit> {
        val store = RecordingStore()
        val d = def(syncTask("a", "http://svc/a", next = "nap"), NodeDefinition.Sleep("nap", 60_000, next = null))
        val asleep = v2Execution(d, state = ExecutionState.Sleeping(Instant.now().plusSeconds(60), null, listOf("a"), emptyList()))
        store.saveSleeping(asleep, (asleep.state as ExecutionState.Sleeping).wakeAt)
        // Exactly what RuntimeBootstrap.wakeExecution enqueues:
        val woken = asleep.copy(state = (asleep.state as ExecutionState.Sleeping).copy(wakeAt = Instant.now()))

        val outcome = testExecutor(store, RecordingEnqueuer(), http().provider).execute(woken)

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals("SUCCEEDED", store.finalStatus)
        assertNull(store.sleeping[asleep.id], "the wake-up consumes the sentinel")
    }

    // ── Node duration metric ────────────────────────────────────────────────

    @Test
    fun `node durations are recorded per node kind and mode`() = runBlocking<Unit> {
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        val d = def(
            syncTask("a", "http://svc/a", next = "route"),
            NodeDefinition.Switch("route", listOf(run.trama.saga.SwitchCaseDef("c", JsonPrimitive(true), "nap")), default = "nap"),
            NodeDefinition.Sleep("nap", 60_000, next = null),
        )

        testExecutor(RecordingStore(), RecordingEnqueuer(), http().provider, metrics = run.trama.telemetry.Metrics(registry)).execute(v2Execution(d))

        fun count(kind: String, mode: String) =
            registry.find("saga.node.duration").tags("node_kind", kind, "mode", mode, "saga_name", d.name).timer()?.count() ?: 0L
        assertEquals(1, count("task", "sync"))
        assertEquals(1, count("switch", "none"))
        assertEquals(1, count("sleep", "none"))
    }

    // ── Success / failure hooks ─────────────────────────────────────────────

    @Test
    fun `onSuccessCallback fires with rendered body after the last node`() = runBlocking<Unit> {
        val store = RecordingStore()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a"),
            onSuccess = httpCall("http://hooks/success", body = """{"saga":"{{sagaId}}","order":"{{payload.orderId}}"}"""),
        )
        val exec = v2Execution(d, payload = mapOf("orderId" to PayloadValue(JsonPrimitive("o-1"))))

        testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec)

        assertEquals(listOf("/a", "/success"), http.urls())
        assertEquals("""{"saga":"${exec.id}","order":"o-1"}""", String(http.requests.last().body.toByteArray()))
        assertEquals("SUCCEEDED", store.finalStatus)
        assertTrue(store.callbackWarnings.isEmpty())
    }

    @Test
    fun `a Succeeded execution from a terminal async callback is finished with hooks`() = runBlocking<Unit> {
        val store = RecordingStore()
        val http = http()
        val d = def(syncTask("a", "http://svc/a"), onSuccess = httpCall("http://hooks/success"))
        val fromCallback = v2Execution(d, state = ExecutionState.Succeeded(Instant.now()))

        val outcome = testExecutor(store, RecordingEnqueuer(), http.provider).execute(fromCallback)

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals("SUCCEEDED", store.finalStatus)
        assertEquals(listOf("/success"), http.urls(), "only the hook runs; no node is re-executed")
    }

    @Test
    fun `failing onSuccessCallback records a warning but saga still succeeds`() = runBlocking<Unit> {
        val store = RecordingStore()
        val d = def(syncTask("a", "http://svc/a"), onSuccess = httpCall("http://hooks/fail"))

        val outcome = testExecutor(store, RecordingEnqueuer(), http().provider).execute(v2Execution(d))

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals("SUCCEEDED", store.finalStatus)
        assertTrue(store.callbackWarnings.single().contains("status=500"), store.callbackWarnings.toString())
    }

    @Test
    fun `unreachable onSuccessCallback records a warning with the error`() = runBlocking<Unit> {
        val store = RecordingStore()
        val http = RecordingHttp { req ->
            if (req.url.host == "hooks") throw java.net.ConnectException("refused")
            respond("", HttpStatusCode.OK)
        }
        val d = def(syncTask("a", "http://svc/a"), onSuccess = httpCall("http://hooks/x"))

        testExecutor(store, RecordingEnqueuer(), http.provider).execute(v2Execution(d))

        assertEquals("SUCCEEDED", store.finalStatus)
        assertTrue(store.callbackWarnings.single().contains("refused"))
    }

    @Test
    fun `onFailureCallback fires after compensation and saga ends FAILED`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/undo-a")),
            syncTask("b", "http://svc/fail-b"),
            onFailure = httpCall("http://hooks/on-error", body = """{"saga":"{{sagaId}}"}"""),
        )
        val executor = testExecutor(store, enqueuer, http.provider)

        executor.execute(v2Execution(d))
        val compensating = enqueuer.enqueued.single().execution
        assertIs<ExecutionState.Compensating>(compensating.state)
        val outcome = executor.execute(compensating)

        assertEquals(ExecutionOutcome.FailedFinal, outcome)
        assertEquals(listOf("/a", "/fail-b", "/undo-a", "/on-error"), http.urls())
        assertEquals("FAILED", store.finalStatus)
        assertTrue(store.callbackWarnings.isEmpty())
    }

    // ── Compensation edge cases ─────────────────────────────────────────────

    @Test
    fun `compensation runs in reverse order and skips nodes without compensation`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/undo-a")),
            syncTask("b", "http://svc/b", next = "c"),
            syncTask("c", "http://svc/c", next = "d", compensation = httpCall("http://svc/undo-c")),
            syncTask("d", "http://svc/fail-d"),
        )
        val executor = testExecutor(store, enqueuer, http.provider)

        executor.execute(v2Execution(d))
        executor.execute(enqueuer.enqueued.last().execution)

        assertEquals(listOf("/a", "/b", "/c", "/fail-d", "/undo-c", "/undo-a"), http.urls())
        assertEquals("FAILED", store.finalStatus)
        val downSteps = store.stepResults.filter { it.phase == ExecutionPhase.DOWN }.map { it.stepName }
        assertEquals(listOf("c", "a"), downSteps)
    }

    @Test
    fun `compensation that keeps failing ends CORRUPTED`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/fail-undo-a")),
            syncTask("b", "http://svc/fail-b"),
        )
        val executor = testExecutor(store, enqueuer, http().provider)

        executor.execute(v2Execution(d))
        val outcome = executor.execute(enqueuer.enqueued.last().execution)

        assertEquals(ExecutionOutcome.FailedFinal, outcome)
        assertEquals("CORRUPTED", store.finalStatus)
    }

    @Test
    fun `compensation failure is retried per failureHandling before giving up`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/fail-undo-a")),
            syncTask("b", "http://svc/fail-b"),
            failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 5),
        )
        val executor = testExecutor(store, enqueuer, http().provider)

        executor.execute(v2Execution(d))                          // b fails → forward retry
        executor.execute(enqueuer.enqueued.last().execution)      // b fails again → compensate
        val compensating = enqueuer.enqueued.last().execution
        assertIs<ExecutionState.Compensating>(compensating.state)
        executor.execute(compensating)                            // undo-a fails → compensation retry
        val retrying = enqueuer.enqueued.last()
        assertIs<ExecutionState.Compensating>(retrying.execution.state)
        assertEquals(5, retrying.delayMillis)
        assertNull(store.finalStatus)

        executor.execute(retrying.execution)                      // undo-a fails again → CORRUPTED
        assertEquals("CORRUPTED", store.finalStatus)
    }

    @Test
    fun `a delayed retry checkpoints the time it is due`() = runBlocking<Unit> {
        val store = RecordingStore()
        val d = def(syncTask("a", "http://svc/fail-a"), failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 60_000))
        val before = Instant.now()

        testExecutor(store, RecordingEnqueuer(), http().provider).execute(v2Execution(d))

        // The reconciler leaves an execution alone until then: re-sending it earlier would run
        // the retry before its delay.
        val due = store.resumeAts.single()
        assertTrue(!due.isBefore(before.plusMillis(60_000)), "due at $due, retry fires 60s after $before")
    }

    @Test
    fun `long compensation stack checkpoints after maxNodesPerExecution`() = runBlocking<Unit> {
        val store = RecordingStore()
        val enqueuer = RecordingEnqueuer()
        val http = http()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/undo-a")),
            syncTask("b", "http://svc/b", next = "c", compensation = httpCall("http://svc/undo-b")),
            syncTask("c", "http://svc/fail-c"),
        )
        val forward = testExecutor(store, enqueuer, http.provider)
        forward.execute(v2Execution(d))
        val compensating = enqueuer.enqueued.last().execution

        val sliced = testExecutor(store, enqueuer, http.provider, maxNodesPerExecution = 1)
        val outcome = sliced.execute(compensating)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        val next = assertIs<ExecutionState.Compensating>(enqueuer.enqueued.last().execution.state)
        assertEquals(listOf("a"), next.compensationStack)
        assertNull(store.finalStatus)

        sliced.execute(enqueuer.enqueued.last().execution)
        assertEquals("FAILED", store.finalStatus)
        assertEquals(listOf("/undo-b", "/undo-a"), http.urls().filter { it.startsWith("/undo") })
    }
}
