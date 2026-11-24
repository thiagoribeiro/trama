package run.trama.saga.store

import java.time.Instant
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.JoinBranchLink
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.Parking
import run.trama.saga.PayloadValue
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaExecution
import run.trama.saga.StepRecord
import run.trama.saga.TaskMode
import run.trama.saga.httpCall

/** The checkpoint SQL: compare-and-set writes, terminal protection and redelivery claims. */
class CheckpointRepositoryIntegrationTest {
    private lateinit var repo: SagaRepository

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        repo = SagaRepository(IntegrationDb.client)
    }

    private fun execution(): SagaExecution {
        val def = SagaDefinitionV2(
            name = "ckpt-${UUID.randomUUID()}",
            version = "1",
            failureHandling = FailureHandling.Retry(0, 0),
            entrypoint = "a",
            nodes = listOf(NodeDefinition.Task("a", NodeActionDef(TaskMode.SYNC, httpCall("http://svc/a")), next = "b"),
                NodeDefinition.Task("b", NodeActionDef(TaskMode.SYNC, httpCall("http://svc/b")))),
        )
        return SagaExecution(
            definition = SagaDefinition(def.name, def.version, def.failureHandling, steps = emptyList()),
            definitionV2 = def,
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "a"),
            payload = mapOf("order" to PayloadValue(JsonPrimitive("o-1"))),
        )
    }

    private fun step(name: String) = StepRecord(0, name, ExecutionPhase.UP, 200, true, """{"ok":true}""", Instant.now())

    private fun SagaExecution.next(state: ExecutionState) = copy(state = state, checkpointSeq = checkpointSeq + 1)

    private suspend fun age(id: UUID, seconds: Long, resumeAtSecondsAgo: Long = seconds) =
        IntegrationDb.client.withConnection { c ->
            c.prepareStatement(
                "UPDATE saga_execution SET updated_at = now() - make_interval(secs => ?), resume_at = now() - make_interval(secs => ?) WHERE id = ?"
            ).use { ps ->
                ps.setLong(1, seconds); ps.setLong(2, resumeAtSecondsAgo); ps.setObject(3, id); ps.executeUpdate()
            }
        }

    @Test
    fun `a checkpoint only applies on top of the seq it was based on`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val next = exec.next(ExecutionState.InProgress(activeNodeId = "b", completedNodes = listOf("a")))

        assertTrue(repo.checkpoint(next, next.checkpointSeq, Instant.now(), step("a"), null))
        assertFalse(repo.checkpoint(next, next.checkpointSeq, Instant.now(), step("a"), null), "a second copy based on seq 0 loses")

        assertEquals(1, repo.getStepResults(exec.id).size, "the losing copy's step is not recorded")
        val persisted = assertNotNull(repo.readCheckpoint(exec.id))
        assertEquals(1, persisted.seq)
        assertEquals(1, persisted.carrier)
        assertFalse(persisted.legacy)
    }

    @Test
    fun `a stored checkpoint rebuilds the execution`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val next = exec.next(ExecutionState.InProgress(activeNodeId = "b", completedNodes = listOf("a")))
        repo.checkpoint(next, 0, Instant.now(), step("a"), null)

        val loaded = assertNotNull(repo.loadCheckpoint(exec.id))

        assertEquals(next.state, loaded.state)
        assertEquals(next.payload, loaded.payload)
        assertEquals(next.definitionV2, loaded.definitionV2)
        assertEquals(1, loaded.checkpointSeq)
    }

    @Test
    fun `a finished execution can be neither checkpointed nor finalized again`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))

        assertTrue(repo.finalizeCheckpointed(exec.id, 0, "SUCCEEDED", null))
        assertFalse(repo.finalizeCheckpointed(exec.id, 1, "FAILED", "zombie"), "SUCCEEDED is never overwritten")
        assertFalse(repo.checkpoint(exec.copy(checkpointSeq = 2), 2, Instant.now(), null, null))

        assertEquals("SUCCEEDED", repo.getExecutionStatus(exec.id)?.status)
        assertNull(repo.prepareRetry(exec), "only FAILED executions are retried")
    }

    @Test
    fun `parking records the parked state with the checkpoint`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val wakeAt = Instant.now().plusSeconds(60)
        val asleep = exec.next(ExecutionState.Sleeping(wakeAt, "b", listOf("a"), emptyList()))

        repo.checkpoint(asleep, asleep.checkpointSeq, wakeAt, null, Parking.Sleep(wakeAt))

        assertEquals("SLEEPING", repo.getExecutionStatus(exec.id)?.status)
        assertEquals(asleep.id, repo.peekSleepingState(exec.id)?.execution?.id)
    }

    @Test
    fun `a stalled running execution is claimed once, at a new seq`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        age(exec.id, seconds = 600)

        val claimed = repo.claimStalledExecutions(staleAfterMillis = 120_000, limit = 1000).filter { it.id == exec.id }

        assertEquals(1, claimed.single().checkpointSeq, "the seq is bumped so older copies become stale")
        assertIs<ExecutionState.InProgress>(claimed.single().state)
        assertTrue(repo.claimStalledExecutions(120_000, 1000).none { it.id == exec.id }, "claimed rows are fresh again")
    }

    @Test
    fun `an execution waiting out a retry delay is not stalled`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        // Last written 10 minutes ago, but due only in 10 minutes.
        age(exec.id, seconds = 600, resumeAtSecondsAgo = -600)

        assertTrue(repo.claimStalledExecutions(120_000, 1000).none { it.id == exec.id })
    }

    @Test
    fun `an expired callback wait is claimed for its timeout`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val deadline = Instant.now().minusSeconds(600)
        val waiting = exec.next(ExecutionState.WaitingCallback("a", 0, deadline, "n-1", emptyList(), emptyList()))
        repo.checkpoint(waiting, waiting.checkpointSeq, deadline, step("a"), Parking.Callback("sig"))

        val claimed = repo.claimExpiredCallbackWaits(bufferSeconds = 120, limit = 1000).filter { it.id == exec.id }

        assertIs<ExecutionState.WaitingCallback>(claimed.single().state)
        assertEquals(2, claimed.single().checkpointSeq)
    }

    @Test
    fun `a join whose branches all finished is found even if no arrival was recorded`() = runBlocking<Unit> {
        val parent = execution()
        val children = listOf("x", "y").map { b ->
            parent.copy(id = UUID.randomUUID(), parentExecutionId = parent.id, parentStartedAt = parent.startedAt, parentSplitNodeId = "fan", branchId = b)
        }
        repo.admitExecutions(listOf(parent) + children)
        repo.registerJoinBarrier(parent.id, parent.startedAt, "fan", "fan-in", children.map { JoinBranchLink(it.branchId!!, it.id, it.startedAt) })
        val waiting = parent.next(ExecutionState.WaitingJoin("fan", "fan-in", 2, emptyList(), emptyList()))
        repo.checkpoint(waiting, waiting.checkpointSeq, Instant.now(), null, Parking.Join)

        repo.finalizeCheckpointed(children[0].id, 0, "SUCCEEDED", null)
        assertTrue(parent.id !in repo.findStalledJoinBarriers(1000), "one branch is still running")

        // The second branch finishes, but its process dies before recording the arrival.
        repo.finalizeCheckpointed(children[1].id, 0, "FAILED", "boom")
        assertTrue(parent.id in repo.findStalledJoinBarriers(1000))
    }

    @Test
    fun `responses that are not JSON are stored as JSON strings`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val bodies = listOf("ok", "<html>hi</html>", "{oops", "{\"a\": ok}", "")
        repo.insertStepCalls(bodies.map { body ->
            run.trama.saga.StepCallEntry(exec.id, exec.startedAt, "a", ExecutionPhase.UP, 0, "http://svc/a", body, 200, body, null, Instant.now())
        })
        val next = exec.next(ExecutionState.InProgress(activeNodeId = "b", completedNodes = listOf("a")))
        assertTrue(repo.checkpoint(next, next.checkpointSeq, Instant.now(), step("a").copy(responseBody = "ok"), null))

        assertEquals(JsonPrimitive("ok"), repo.loadStepResultsForTemplate(exec.id).single().upBody)
        assertEquals(bodies.size, repo.getStepCalls(exec.id).size)
    }

    @Test
    fun `JSON responses keep their structure`() = runBlocking<Unit> {
        val exec = execution()
        repo.admitExecutions(listOf(exec))
        val next = exec.next(ExecutionState.InProgress(activeNodeId = "b", completedNodes = listOf("a")))
        repo.checkpoint(next, next.checkpointSeq, Instant.now(), step("a").copy(responseBody = """{"id": 7, "ok": true, "tags": ["x", null, 1.5]}"""), null)

        val body = repo.loadStepResultsForTemplate(exec.id).single().upBody
        assertEquals("""{"id":7,"ok":true,"tags":["x",null,1.5]}""".let { kotlinx.serialization.json.Json.parseToJsonElement(it) }, body)
    }
}
