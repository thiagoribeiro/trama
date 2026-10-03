package run.trama.saga.store

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.JoinBranchLink
import run.trama.saga.StepCallEntry

class SagaRepositoryIntegrationTest {
    private lateinit var repo: SagaRepository

    @BeforeTest
    fun setUp() {
        IntegrationDb.assumeDocker()
        repo = SagaRepository(IntegrationDb.client)
    }

    private val defJson = """{"name":"x","version":"1","failureHandling":{"type":"retry","maxAttempts":1,"delayMillis":0},"steps":[]}"""

    private suspend fun newExecution(name: String, startedAt: Instant = Instant.now()): UUID {
        val id = UUID.randomUUID()
        repo.upsertExecutionRecord(id, name, "1", defJson, startedAt)
        return id
    }

    @Test
    fun `listExecutions filters by name and status and orders newest first`() = runBlocking<Unit> {
        val name = "list-${UUID.randomUUID()}"
        val now = Instant.now()
        val oldest = newExecution(name, now.minusSeconds(30))
        val middle = newExecution(name, now.minusSeconds(20))
        val newest = newExecution(name, now.minusSeconds(10))
        newExecution("other-${UUID.randomUUID()}")
        repo.updateExecutionFinal(middle, "FAILED", "boom")

        val all = repo.listExecutions(name = name)
        assertEquals(listOf(newest, middle, oldest), all.map { it.id })

        val failed = repo.listExecutions(status = "FAILED", name = name)
        assertEquals(listOf(middle), failed.map { it.id })
        assertEquals("boom", failed.single().failureDescription)
        assertNotNull(failed.single().completedAt)

        assertEquals(listOf(middle), repo.listExecutions(name = name, limit = 1, offset = 1).map { it.id })
    }

    @Test
    fun `upsert on an existing execution resets it to IN_PROGRESS and keeps the stored definition`() = runBlocking<Unit> {
        val name = "upsert-${UUID.randomUUID()}"
        val startedAt = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val id = newExecution(name, startedAt)
        repo.updateStatus(id, "SLEEPING")
        assertEquals("SLEEPING", repo.getExecutionStatus(id)?.status)

        repo.upsertExecutionRecord(id, name, "1", """{"other":true}""", startedAt)

        val status = assertNotNull(repo.getExecutionStatus(id))
        assertEquals("IN_PROGRESS", status.status)
        assertTrue(status.definition!!.contains("failureHandling"), "original definition must be preserved")
    }

    @Test
    fun `failure, warning and retry bookkeeping round-trip`() = runBlocking<Unit> {
        val id = newExecution("retry-${UUID.randomUUID()}")
        repo.updateFailureDescription(id, "step 2 failed", failedStepIndex = 2, failedPhase = ExecutionPhase.UP)
        repo.updateCallbackWarning(id, "hook 500")
        repo.updateExecutionFinal(id, "FAILED", "step 2 failed")

        val status = assertNotNull(repo.getExecutionStatus(id))
        assertEquals("FAILED", status.status)
        assertEquals("hook 500", status.callbackWarning)
        assertEquals(2, status.lastFailedStepIndex)
        assertEquals("UP", status.lastFailedPhase)

        val retry = assertNotNull(repo.getExecutionForRetry(id))
        assertEquals(2, retry.failedStepIndex)
        assertTrue(retry.definitionJson!!.contains("failureHandling"))

        val retryExecution = SagaExecution(
            definition = SagaDefinition("x", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
            id = id,
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "a"),
        )
        val prepared = assertNotNull(repo.prepareRetry(retryExecution))
        assertEquals(1, prepared.checkpointSeq, "retry bumps the seq, fencing off the old run")
        assertNull(repo.prepareRetry(retryExecution), "only a FAILED execution can be retried")
        val after = assertNotNull(repo.getExecutionStatus(id))
        assertEquals("IN_PROGRESS", after.status)
        assertNull(after.failureDescription)
        assertNull(after.callbackWarning)
        assertNull(after.lastFailedStepIndex)
    }

    @Test
    fun `unknown execution id returns null everywhere`() = runBlocking<Unit> {
        val id = UUID.randomUUID()
        assertNull(repo.getExecutionStatus(id))
        assertNull(repo.getExecutionForRetry(id))
        assertEquals(emptyList(), repo.getStepResults(id))
        assertEquals(emptyList(), repo.getStepCalls(id))
    }

    @Test
    fun `executions older than the 15-day read window are invisible to status lookups`() = runBlocking<Unit> {
        // Documents the hard-coded cutoff(): executions still exist but can no longer be queried
        // (and therefore not retried or woken) once they are older than 15 days.
        val id = newExecution("old-${UUID.randomUUID()}", Instant.now().minus(16, ChronoUnit.DAYS))
        assertNull(repo.getExecutionStatus(id))
    }

    @Test
    fun `step results and step calls are returned in insertion order with parsed fields`() = runBlocking<Unit> {
        val startedAt = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val id = newExecution("steps-${UUID.randomUUID()}", startedAt)
        repo.insertStepResult(id, startedAt, 0, "a", ExecutionPhase.UP, 200, true, """{"x":1}""", startedAt)
        repo.insertStepResult(id, startedAt, 1, "b", ExecutionPhase.UP, 500, false, "not json", startedAt)
        repo.insertStepResult(id, startedAt, 1, "b", ExecutionPhase.DOWN, 200, true, null, startedAt)
        repo.insertStepCalls(
            listOf(
                StepCallEntry(id, startedAt, "a", ExecutionPhase.UP, 0, "http://svc/a", """{"in":1}""", 200, """{"x":1}""", null, startedAt),
                StepCallEntry(id, startedAt, "b", ExecutionPhase.UP, 1, "http://svc/b", null, null, null, "connection refused", startedAt.plusMillis(5)),
            )
        )

        val results = repo.getStepResults(id)
        assertEquals(listOf("a:UP", "b:UP", "b:DOWN"), results.map { "${it.stepName}:${it.phase}" })
        assertEquals(listOf(true, false, true), results.map { it.success })
        assertTrue(results[0].responseBody!!.contains("\"x\""))
        assertNotNull(results[1].responseBody, "non-JSON bodies must still be stored (as a JSON string)")

        val calls = repo.getStepCalls(id)
        assertEquals(listOf("a", "b"), calls.map { it.stepName })
        assertEquals("http://svc/a", calls[0].requestUrl)
        assertEquals(1, calls[1].attempt)
        assertEquals("connection refused", calls[1].error)
        assertNull(calls[1].statusCode)

        val template = repo.loadStepResultsForTemplate(id)
        assertTrue(template.any { it.name == "a" && it.upBody != null })
    }

    @Test
    fun `stalled join barrier is found only while the parent is still WAITING_JOIN`() = runBlocking<Unit> {
        val startedAt = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val parent = newExecution("join-${UUID.randomUUID()}", startedAt)
        val children = List(2) { UUID.randomUUID() }
        repo.registerJoinBarrier(
            parent, startedAt, "split", "join",
            children.mapIndexed { i, c -> JoinBranchLink("b$i", c, startedAt) },
        )
        repo.updateStatus(parent, "WAITING_JOIN")

        repo.markChildArrived(parent, startedAt, "split", children[0])
        assertTrue(parent !in repo.findStalledJoinBarriers(1000), "barrier not yet satisfied")

        val arrival = assertNotNull(repo.markChildArrived(parent, startedAt, "split", children[1]))
        assertEquals(2, arrival.arrived)
        assertTrue(parent in repo.findStalledJoinBarriers(1000))

        repo.updateStatus(parent, "IN_PROGRESS")
        assertTrue(parent !in repo.findStalledJoinBarriers(1000), "already resumed parents are not stalled")
    }

    @Test
    fun `definitions list, lookup by name-version and delete`() = runBlocking<Unit> {
        val name = "def-${UUID.randomUUID()}"
        val id = UUID.randomUUID()
        assertTrue(repo.insertDefinition(id, name, "1", defJson))
        assertTrue(!repo.insertDefinition(UUID.randomUUID(), name, "1", defJson), "name+version must be unique")

        assertEquals(id, repo.getDefinitionByNameVersion(name, "1")?.id)
        assertTrue(repo.listDefinitions(limit = 200).any { it.id == id })

        assertTrue(repo.deleteDefinition(id))
        assertNull(repo.getDefinition(id))
        assertNull(repo.getDefinitionByNameVersion(name, "1"))
        assertTrue(!repo.deleteDefinition(id))
    }

    @Test
    fun `POSTGRES store keeps a sleep sentinel that can be peeked and consumed once`() = runBlocking<Unit> {
        val pgStore = run.trama.saga.SagaRepositoryStore(repo)
        val startedAt = Instant.now().truncatedTo(ChronoUnit.MICROS)
        val id = newExecution("pg-sleep-${UUID.randomUUID()}", startedAt)
        val wakeAt = Instant.now().plusSeconds(3600)
        val exec = run.trama.saga.SagaExecution(
            definition = run.trama.saga.SagaDefinition("pg-sleep", "1", run.trama.saga.FailureHandling.Retry(1, 0), steps = emptyList()),
            id = id,
            startedAt = startedAt,
            currentStepIndex = 0,
            state = run.trama.saga.ExecutionState.Sleeping(wakeAt, "next", emptyList(), emptyList()),
        )

        pgStore.saveSleeping(exec, wakeAt)

        assertEquals("SLEEPING", repo.getExecutionStatus(id)?.status)
        assertNotNull(pgStore.peekSleeping(id), "sentinel must exist while sleeping")
        assertNotNull(pgStore.consumeSleeping(id))
        assertNull(pgStore.consumeSleeping(id))
    }

    @Test
    fun `a definition deleted by another pod stops being served once the cache TTL elapses`() = runBlocking<Unit> {
        // The per-pod cache is only invalidated locally; its TTL bounds cross-pod staleness.
        val podA = SagaRepository(IntegrationDb.client, definitionCacheTtlMillis = 200)
        val podB = SagaRepository(IntegrationDb.client, definitionCacheTtlMillis = 200)
        val name = "stale-${UUID.randomUUID()}"
        val id = UUID.randomUUID()
        podA.insertDefinition(id, name, "1", defJson)
        assertNotNull(podA.getDefinition(id))

        assertTrue(podB.deleteDefinition(id))
        kotlinx.coroutines.delay(300)

        assertNull(podA.getDefinition(id), "pod A still serves the deleted definition after the TTL")
        assertNull(podA.getDefinitionByNameVersion(name, "1"))
    }
}
