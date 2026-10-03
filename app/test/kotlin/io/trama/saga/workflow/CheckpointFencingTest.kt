package run.trama.saga.workflow

import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import run.trama.saga.CheckpointingStore
import run.trama.saga.ClaimLease
import run.trama.saga.ExecutionOutcome
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.LeaseLostException
import run.trama.saga.NodeDefinition
import run.trama.saga.RecordingEnqueuer
import run.trama.saga.RecordingHttp
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.StaleCheckpointException
import run.trama.saga.httpCall
import run.trama.saga.syncTask
import run.trama.saga.testExecutor
import run.trama.saga.v2Execution

/**
 * Fencing and redelivery: the executor only acts on the copy of an execution that matches its
 * durable checkpoint, and a worker that lost its claim stops before any further side effect.
 */
class CheckpointFencingTest {
    private fun http(onRequest: (String) -> Unit = {}) = RecordingHttp { req ->
        onRequest(req.url.encodedPath)
        if (req.url.encodedPath.contains("/fail")) respond("""{"error":"x"}""", HttpStatusCode.InternalServerError)
        else respond("""{"ok":true}""", HttpStatusCode.OK)
    }

    private fun RecordingHttp.paths() = requests.map { it.url.encodedPath }

    private fun def(vararg nodes: NodeDefinition, maxAttempts: Int = 0) =
        SagaDefinitionV2("fence-${java.util.UUID.randomUUID()}", "1", FailureHandling.Retry(maxAttempts, 0), nodes.first().id, nodes.toList())

    private fun chain() = def(
        syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/undo-a")),
        syncTask("b", "http://svc/b", next = "c", compensation = httpCall("http://svc/undo-b")),
        syncTask("c", "http://svc/c"),
    )

    @Test
    fun `each completed node advances the durable checkpoint`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val exec = v2Execution(chain())
        store.admit(listOf(exec))

        assertEquals(ExecutionOutcome.Succeeded, testExecutor(store, RecordingEnqueuer(), http().provider).execute(exec))

        assertEquals(listOf(1L, 2L, 3L), store.checkpoints.map { it.checkpointSeq })
        assertEquals("SUCCEEDED", store.rows.getValue(exec.id).status)
    }

    @Test
    fun `a copy behind the checkpoint and not its carrier is dropped before any call`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val exec = v2Execution(chain())
        // Another queue item (seq 5) now carries the execution.
        store.rows[exec.id] = CheckpointingStore.Row("IN_PROGRESS", 5, 5, exec.copy(checkpointSeq = 5))
        val http = http()

        assertFailsWith<StaleCheckpointException> {
            testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec)
        }
        assertEquals(emptyList(), http.paths())
    }

    @Test
    fun `a redelivered carrier resumes from the checkpoint its dead worker reached`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val item = v2Execution(chain())
        // The previous worker ran a and b (checkpoints 1 and 2) inline, then died.
        val reached = item.copy(
            state = ExecutionState.InProgress(activeNodeId = "c", completedNodes = listOf("a", "b"), compensationStack = listOf("b", "a")),
            checkpointSeq = 2,
        )
        store.rows[item.id] = CheckpointingStore.Row("IN_PROGRESS", 2, carrier = 0, execution = reached)
        val http = http()

        val outcome = testExecutor(store, RecordingEnqueuer(), http.provider).execute(item)

        assertEquals(ExecutionOutcome.Succeeded, outcome)
        assertEquals(listOf("/c"), http.paths(), "a and b are not repeated")
    }

    @Test
    fun `a finished execution is never reopened by an old copy`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val exec = v2Execution(chain())
        store.rows[exec.id] = CheckpointingStore.Row("SUCCEEDED", 4, null, null)
        val http = http()

        assertFailsWith<StaleCheckpointException> {
            testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec)
        }
        assertEquals(emptyList(), http.paths())
        assertNull(store.finalStatus)
    }

    @Test
    fun `a worker overtaken mid-node cannot fail or compensate the execution`() = runBlocking<Unit> {
        // The zombie case: while this worker's call to c is in flight (and times out), another
        // worker takes the execution over and finishes it.
        val store = CheckpointingStore()
        val d = def(
            syncTask("a", "http://svc/a", next = "b", compensation = httpCall("http://svc/undo-a")),
            syncTask("b", "http://svc/fail-b"),
        )
        val exec = v2Execution(d)
        store.admit(listOf(exec))
        val http = http { path ->
            if (path == "/fail-b") store.rows[exec.id] = CheckpointingStore.Row("SUCCEEDED", 9, null, null)
        }

        assertFailsWith<StaleCheckpointException> {
            testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec)
        }
        assertEquals(listOf("/a", "/fail-b"), http.paths(), "no compensation call")
        assertNull(store.finalStatus)
        assertEquals("SUCCEEDED", store.rows.getValue(exec.id).status)
    }

    @Test
    fun `a worker whose claim lease ran out stops before the next call`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val exec = v2Execution(chain())
        store.admit(listOf(exec))
        val expired = ClaimLease(deadlineMillis = System.currentTimeMillis() - 1, marginMillis = 0)
        val http = http()

        assertFailsWith<LeaseLostException> {
            withContext(expired) { testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec) }
        }
        assertEquals(emptyList(), http.paths())
        assertEquals(0L, store.rows.getValue(exec.id).seq)
    }

    @Test
    fun `a lease marked lost by the heartbeat fences the worker at its next checkpoint`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val exec = v2Execution(chain())
        store.admit(listOf(exec))
        val lease = ClaimLease(deadlineMillis = System.currentTimeMillis() + 60_000, marginMillis = 0)
        val http = http { path -> if (path == "/a") lease.lost = true }

        assertFailsWith<LeaseLostException> {
            withContext(lease) { testExecutor(store, RecordingEnqueuer(), http.provider).execute(exec) }
        }
        assertEquals(listOf("/a"), http.paths())
        assertEquals(0L, store.rows.getValue(exec.id).seq, "the result of a is not recorded by a fenced worker")
    }

    @Test
    fun `parking hands the execution to the enqueued item`() = runBlocking<Unit> {
        val store = CheckpointingStore()
        val enqueuer = RecordingEnqueuer()
        val d = def(syncTask("a", "http://svc/a", next = "nap"), NodeDefinition.Sleep("nap", 60_000, next = null))
        val exec = v2Execution(d)
        store.admit(listOf(exec))

        testExecutor(store, enqueuer, http().provider).execute(exec)

        val parked = enqueuer.enqueued.single().execution
        val row = store.rows.getValue(exec.id)
        assertEquals("SLEEPING", row.status)
        assertEquals(parked.checkpointSeq, row.seq)
        assertEquals(parked.checkpointSeq, row.carrier, "the sleep's queue item now carries the execution")
    }
}
