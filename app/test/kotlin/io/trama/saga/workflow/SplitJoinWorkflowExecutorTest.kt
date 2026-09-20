package run.trama.saga.workflow

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import run.trama.saga.ChildExecutionStatus
import run.trama.saga.DefaultRetryPolicy
import run.trama.saga.ExecutionOutcome
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpClientProvider
import run.trama.saga.HttpVerb
import run.trama.saga.JoinArrival
import run.trama.saga.JoinBranchLink
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.PayloadValue
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.SleepEntry
import run.trama.saga.StepCallEntry
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import run.trama.saga.WaitingInfo
import run.trama.telemetry.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

/**
 * Exercises real [WorkflowExecutor] split/join behavior end-to-end at the unit level —
 * no Postgres/Redis, just the [SagaExecutionStore]/[SagaEnqueuer] interfaces backed by
 * an in-memory fake. This is deliberately the most load-bearing test for this feature:
 * it is runnable without Docker and pins down the exact spawn → arrive → resume contract.
 */
class SplitJoinWorkflowExecutorTest {

    private fun httpCall(url: String) = HttpCall(TemplateString(url), HttpVerb.GET)

    /** A definition with a two-branch split feeding into a join that continues to "after-join". */
    private fun splitJoinDefinition(name: String): SagaDefinitionV2 = SagaDefinitionV2(
        name = name,
        version = "1",
        failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
        entrypoint = "fan-out",
        nodes = listOf(
            NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "fan-in"),
            NodeDefinition.Task("branch-a", NodeActionDef(TaskMode.SYNC, httpCall("http://a"))),
            NodeDefinition.Task("branch-b", NodeActionDef(TaskMode.SYNC, httpCall("http://b"))),
            NodeDefinition.Join(id = "fan-in", next = "after-join"),
            NodeDefinition.Task("after-join", NodeActionDef(TaskMode.SYNC, httpCall("http://after"))),
        ),
    )

    private fun executionOf(defV2: SagaDefinitionV2, activeNodeId: String): SagaExecution = SagaExecution(
        definition = SagaDefinition(
            name = defV2.name,
            version = defV2.version,
            failureHandling = defV2.failureHandling,
            steps = emptyList(),
        ),
        definitionV2 = defV2,
        id = UUID.randomUUID(),
        startedAt = Instant.now(),
        currentStepIndex = 0,
        state = ExecutionState.InProgress(activeNodeId = activeNodeId),
        payload = mapOf(),
    )

    private fun newExecutor(store: SagaExecutionStore, enqueuer: FakeEnqueuer, ok: Boolean = true) = WorkflowExecutor(
        store = store,
        renderer = MustacheTemplateRenderer(),
        retryPolicy = DefaultRetryPolicy(),
        enqueuer = enqueuer,
        httpClient = FakeHttpClientProvider(
            HttpClient(
                MockEngine {
                    if (ok) respond("""{"ok":true}""", HttpStatusCode.OK, headersOf("Content-Type" to listOf("application/json")))
                    else respond("""{"error":true}""", HttpStatusCode.InternalServerError)
                },
            ),
        ),
        metrics = Metrics(SimpleMeterRegistry()),
    )

    @Test
    fun `split spawns one child per branch and parks the parent in WaitingJoin`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("spawn-test")
        val parent = executionOf(def, "fan-out")

        val outcome = executor.execute(parent)

        assertEquals(ExecutionOutcome.Reenqueued, outcome)
        assertEquals(2, enqueuer.enqueued.size, "expected exactly the 2 branch children to be enqueued")
        val branchIds = enqueuer.enqueued.map { (it.state as ExecutionState.InProgress).activeNodeId }.toSet()
        assertEquals(setOf("branch-a", "branch-b"), branchIds)
        enqueuer.enqueued.forEach { child ->
            assertEquals(parent.id, child.parentExecutionId)
            assertEquals("fan-out", child.parentSplitNodeId)
            assertEquals("fan-in", child.parentJoinNodeId)
        }

        val waiting = store.waitingJoins[parent.id]
        assertNotNull(waiting, "parent must be parked as WaitingJoin")
        val waitingState = waiting.state as ExecutionState.WaitingJoin
        assertEquals("fan-out", waitingState.splitNodeId)
        assertEquals("fan-in", waitingState.joinNodeId)
        assertEquals(2, waitingState.expectedBranches)

        val splitStep = store.stepResults.getValue(parent.id).single { it.stepName == "fan-out" }
        assertEquals(ExecutionPhase.SPLIT, splitStep.phase)
    }

    @Test
    fun `join only fires once the last branch arrives, then resumes the parent`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("resume-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        // First branch finishes: barrier not yet satisfied, parent must stay parked.
        executor.execute(branchA)
        assertTrue(enqueuer.enqueued.isEmpty(), "parent must not resume after only 1 of 2 branches arrived")
        assertNotNull(store.waitingJoins[parent.id], "parent must still be parked")

        // Second (last) branch finishes: barrier satisfied, parent must resume.
        executor.execute(branchB)
        assertNull(store.waitingJoins[parent.id], "parent's WaitingJoin entry must be consumed exactly once")
        assertEquals(1, enqueuer.enqueued.size, "expected the parent to be re-enqueued after the join fires")

        val resumedParent = enqueuer.enqueued.single()
        assertEquals(parent.id, resumedParent.id)
        val resumedState = resumedParent.state
        assertIs<ExecutionState.InProgress>(resumedState)
        assertEquals("after-join", resumedState.activeNodeId)
        assertTrue("fan-out" in resumedState.completedNodes)
        assertTrue("fan-in" in resumedState.completedNodes)

        val joinStep = store.stepResults.getValue(parent.id).single { it.stepName == "fan-in" }
        assertEquals(ExecutionPhase.JOIN, joinStep.phase)
        val joinBody = Json.parseToJsonElement(requireNotNull(joinStep.responseBody)).jsonObject
        assertEquals(true, joinBody["allSucceeded"]?.jsonPrimitive?.boolean)
        val branchStatuses = joinBody["branches"]!!.jsonArray.map { it.jsonObject["status"]!!.jsonPrimitive.content }
        assertEquals(listOf("SUCCEEDED", "SUCCEEDED"), branchStatuses)
    }

    @Test
    fun `a failed branch is reflected in the join result without blocking the barrier`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val okExecutor = newExecutor(store, enqueuer, ok = true)
        val failExecutor = newExecutor(store, enqueuer, ok = false)
        val def = splitJoinDefinition("partial-failure-test")
        val parent = executionOf(def, "fan-out")

        okExecutor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        okExecutor.execute(branchA)
        // maxAttempts=0 means retries are exhausted immediately, but (mirroring
        // DefaultSagaExecutorTest's retry-exhaustion test) the executor re-enqueues the
        // execution in Compensating state rather than walking it inline — the (empty,
        // since branch-b has no compensation configured) compensation walk that actually
        // finalizes the branch only happens on the *next* execute() call.
        failExecutor.execute(branchB)
        val compensatingBranchB = enqueuer.enqueued.last()
        assertIs<ExecutionState.Compensating>(compensatingBranchB.state)
        enqueuer.enqueued.clear()
        failExecutor.execute(compensatingBranchB)

        assertEquals(1, enqueuer.enqueued.size)
        val joinStep = store.stepResults.getValue(parent.id).single { it.stepName == "fan-in" }
        val joinBody = Json.parseToJsonElement(requireNotNull(joinStep.responseBody)).jsonObject
        assertEquals(false, joinBody["allSucceeded"]?.jsonPrimitive?.boolean)
        val statuses = joinBody["branches"]!!.jsonArray.map { it.jsonObject["status"]!!.jsonPrimitive.content }
        assertEquals(setOf("SUCCEEDED", "FAILED"), statuses.toSet())

        // The parent itself is NOT auto-compensated — it still advances past the join;
        // it is up to the workflow's own downstream logic to react to branch failures.
        val resumedState = enqueuer.enqueued.single().state
        assertIs<ExecutionState.InProgress>(resumedState)
        assertEquals("after-join", resumedState.activeNodeId)
    }

    // ── Regression tests for the code-review fixes ──────────────────────────────

    @Test
    fun `split redelivery reconstructs deterministic children instead of a fresh random batch`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("determinism-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val firstBatch = enqueuer.enqueued.toList()

        // Simulates redelivery: the parent's queue message snapshot predates the split ever
        // running, so a crash/requeue re-executes the SAME SplitNode step from the SAME parent.
        // The split step (re-)computes its children and records them in its own step-result
        // trace on every attempt, regardless of whether they end up (re-)enqueued — this is the
        // observable proof of determinism independent of the enqueue-dedup behavior covered by
        // the "redelivering the split step itself..." test below.
        executor.execute(parent)

        val splitTraces = store.stepResults.getValue(parent.id).filter { it.stepName == "fan-out" }
        assertEquals(2, splitTraces.size, "both attempts must still record their own split trace")
        val firstComputed = Json.parseToJsonElement(requireNotNull(splitTraces[0].responseBody)).jsonArray
        val secondComputed = Json.parseToJsonElement(requireNotNull(splitTraces[1].responseBody)).jsonArray
        val firstByBranch = firstComputed.associate { it.jsonObject["branchId"]!!.jsonPrimitive.content to it.jsonObject["executionId"]!!.jsonPrimitive.content }
        val secondByBranch = secondComputed.associate { it.jsonObject["branchId"]!!.jsonPrimitive.content to it.jsonObject["executionId"]!!.jsonPrimitive.content }
        assertEquals(firstByBranch, secondByBranch, "retrying the split step must recompute the same child ids per branch, not new random ones")

        val branchIds = firstBatch.map { (it.state as ExecutionState.InProgress).activeNodeId }.toSet()
        assertEquals(setOf("branch-a", "branch-b"), branchIds, "the first attempt must still enqueue both branches")
    }

    @Test
    fun `redelivering the split step itself does not re-enqueue already-registered branches`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("split-redelivery-enqueue-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        assertEquals(2, enqueuer.enqueued.size, "the first attempt must enqueue both branches")
        enqueuer.enqueued.clear()

        // Simulates the worker crashing right after the first attempt finished registering and
        // enqueuing both branches, so the parent's own split-step queue message never got acked
        // and is redelivered. Both branches may already be running or even fully finished by
        // now (they could be simple, fast branches) — re-enqueuing them here would restart their
        // entire subtree of work a second time, not just repeat a single call.
        executor.execute(parent)

        assertTrue(
            enqueuer.enqueued.isEmpty(),
            "a redelivered split step must not re-enqueue branches an earlier attempt already registered, got: ${enqueuer.enqueued.size}",
        )
    }

    @Test
    fun `redelivering a branch before its sibling arrives does not inflate the arrival count`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("redelivery-before-sibling-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        executor.execute(branchA)
        // Simulate a redelivery of branch A's own (already finalized) queue message, BEFORE
        // branch B ever runs. With a blind counter this would wrongly count as a 2nd arrival
        // and could satisfy the barrier (arrived == expected == 2) before B ever finished.
        executor.execute(branchA)

        assertTrue(enqueuer.enqueued.isEmpty(), "the parent must not resume before branch B has actually arrived")
        assertNotNull(store.waitingJoins[parent.id], "parent must still be parked, waiting on branch B")

        executor.execute(branchB)
        assertEquals(1, enqueuer.enqueued.size, "the parent must resume exactly once, once branch B genuinely arrives")
    }

    @Test
    fun `a failure inside resumeAfterJoin restores the WaitingJoin pointer instead of losing it`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("resume-failure-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        executor.execute(branchA)
        store.throwOnGetChildStatuses = true
        executor.execute(branchB) // last arrival -> attempts the resume, which now fails

        assertTrue(enqueuer.enqueued.isEmpty(), "the parent must not have been resumed given the injected failure")
        assertNotNull(
            store.waitingJoins[parent.id],
            "the WaitingJoin pointer must be restored after the failed resume attempt, not lost forever",
        )

        // Recovery: once the transient failure clears (e.g. JoinCompletionScanner retrying
        // later), the parent can still be resumed from the restored pointer.
        store.throwOnGetChildStatuses = false
        val resumed = executor.resumeJoinIfSatisfied(parent.id)
        assertTrue(resumed)
        assertEquals(1, enqueuer.enqueued.size)
    }

    @Test
    fun `a failure during the commit phase does not resurrect the WaitingJoin pointer`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("commit-failure-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        executor.execute(branchA)
        // Prepare succeeds (the join's own step result is written, branch statuses read) — the
        // failure only happens in the commit phase (re-enqueueing the resumed parent), which
        // must NOT be treated as retryable: the join itself already succeeded by this point.
        enqueuer.throwOnEnqueue = true
        try {
            executor.execute(branchB)
        } catch (_: Exception) {
            // Expected: a commit-phase failure propagates instead of being swallowed and retried.
        }

        assertNull(
            store.waitingJoins[parent.id],
            "a commit-phase failure must not resurrect the WaitingJoin pointer for an already-decided join",
        )
        val joinStep = store.stepResults.getValue(parent.id).singleOrNull { it.stepName == "fan-in" }
        assertNotNull(joinStep, "the join's own step result must still have been written during prepare")
        Unit
    }

    @Test
    fun `a join whose joinNodeId no longer resolves is finalized as CORRUPTED instead of retried forever`() = runBlocking {
        val store = FakeSplitJoinStore()
        val enqueuer = FakeEnqueuer()
        val executor = newExecutor(store, enqueuer)
        val def = splitJoinDefinition("structural-bug-test")
        val parent = executionOf(def, "fan-out")

        executor.execute(parent)
        val children = enqueuer.enqueued.toList()
        enqueuer.enqueued.clear()
        val branchA = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-a" }
        val branchB = children.single { (it.state as ExecutionState.InProgress).activeNodeId == "branch-b" }

        executor.execute(branchA)

        // Simulate a corrupted/stale WaitingJoin pointer whose joinNodeId no longer resolves to a
        // JoinNode in the workflow — a permanent structural problem, unlike the transient failures
        // covered by the two tests above, so it must never be treated as retryable.
        val parked = store.waitingJoins.getValue(parent.id)
        val corruptedState = (parked.state as ExecutionState.WaitingJoin).copy(joinNodeId = "does-not-exist")
        store.waitingJoins[parent.id] = parked.copy(state = corruptedState)

        executor.execute(branchB) // last arrival -> attempts the resume, which hits the bad joinNodeId

        assertTrue(enqueuer.enqueued.isEmpty(), "a structurally broken join must not be re-enqueued")
        assertNull(
            store.waitingJoins[parent.id],
            "a permanent structural bug must not resurrect the WaitingJoin pointer for endless retry",
        )
        val (status, failureDescription) = store.finalStatuses.getValue(parent.id)
        assertEquals("CORRUPTED", status)
        assertTrue(
            failureDescription?.contains("does-not-exist") == true,
            "failure description should name the missing join node, got: $failureDescription",
        )

        // Confirms there is no infinite-retry loop left behind: the backstop scanner calling
        // resumeJoinIfSatisfied again finds nothing left to resume.
        assertTrue(!executor.resumeJoinIfSatisfied(parent.id))
    }
}

private class FakeHttpClientProvider(override val client: HttpClient) : HttpClientProvider

private class FakeEnqueuer : SagaEnqueuer {
    val enqueued = mutableListOf<SagaExecution>()
    var throwOnEnqueue = false
    override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
        if (throwOnEnqueue) error("simulated failure during the join's commit phase")
        enqueued.add(execution)
    }
}

/** In-memory [SagaExecutionStore] with real (non-stubbed) split/join bookkeeping. */
private class FakeSplitJoinStore : SagaExecutionStore {
    val finalStatuses = mutableMapOf<UUID, Pair<String, String?>>()
    val stepResults = mutableMapOf<UUID, MutableList<RecordedStep>>()
    val waitingJoins = mutableMapOf<UUID, SagaExecution>()
    /** Test hook: makes [getChildStatuses] throw once, to simulate a failure mid-resumeAfterJoin. */
    var throwOnGetChildStatuses = false
    private data class BarrierState(val expected: Int, var arrived: Int = 0)
    private val barriers = mutableMapOf<String, BarrierState>()
    private val arrivedChildIds = mutableMapOf<String, MutableSet<UUID>>()
    private val branchLinks = mutableMapOf<String, List<JoinBranchLink>>()

    data class RecordedStep(val stepName: String, val phase: ExecutionPhase, val responseBody: String?)

    private fun barrierKey(parentId: UUID, splitNodeId: String) = "$parentId|$splitNodeId"

    override suspend fun upsertStart(execution: SagaExecution) {}
    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        finalStatuses[executionId] = status to failureDescription
    }
    override suspend fun updateFailure(executionId: UUID, failureDescription: String, failedStepIndex: Int?, failedPhase: ExecutionPhase?) {}
    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {}
    override suspend fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: Instant?,
    ) {
        stepResults.getOrPut(sagaId) { mutableListOf() }.add(RecordedStep(stepName, phase, responseBody))
    }
    override suspend fun insertStepCalls(calls: List<StepCallEntry>) {}
    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = emptyList()
    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? = null
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: Instant) {}
    override suspend fun peekSleeping(executionId: UUID): SleepEntry? = null
    override suspend fun consumeSleeping(executionId: UUID): SleepEntry? = null
    override suspend fun updateStatus(executionId: UUID, status: String) {}

    override suspend fun registerJoinBarrier(
        parentId: UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        joinNodeId: String,
        branches: List<JoinBranchLink>,
    ): Set<String> {
        // Mirrors the real ON CONFLICT DO NOTHING semantics: a redelivered split step calling
        // this again for the same (parentId, splitNodeId) must not reset already-registered
        // state (arrival progress included), and must report back only the branches that are
        // genuinely new so the caller knows which ones it still needs to enqueue.
        val key = barrierKey(parentId, splitNodeId)
        barriers.getOrPut(key) { BarrierState(expected = branches.size) }
        arrivedChildIds.getOrPut(key) { mutableSetOf() }
        val existingBranchIds = branchLinks[key]?.map { it.branchId }?.toSet() ?: emptySet()
        val newBranches = branches.filter { it.branchId !in existingBranchIds }
        branchLinks[key] = (branchLinks[key] ?: emptyList()) + newBranches
        return newBranches.map { it.branchId }.toSet()
    }

    override suspend fun markChildArrived(parentId: UUID, parentStartedAt: Instant, splitNodeId: String, childId: UUID): JoinArrival? {
        val key = barrierKey(parentId, splitNodeId)
        val barrier = barriers[key] ?: return null
        val arrivedSet = arrivedChildIds.getOrPut(key) { mutableSetOf() }
        val newlyMarked = arrivedSet.add(childId)
        if (newlyMarked) barrier.arrived++
        return JoinArrival(arrived = barrier.arrived, expected = barrier.expected, newlyMarked = newlyMarked)
    }

    override suspend fun getJoinBranches(parentId: UUID, parentStartedAt: Instant, splitNodeId: String): List<JoinBranchLink> =
        branchLinks[barrierKey(parentId, splitNodeId)] ?: emptyList()

    override suspend fun saveWaitingJoin(execution: SagaExecution) {
        waitingJoins[execution.id] = execution
    }

    override suspend fun consumeWaitingJoin(executionId: UUID): SagaExecution? = waitingJoins.remove(executionId)

    override suspend fun getChildStatus(executionId: UUID): ChildExecutionStatus? {
        val (status, failureDescription) = finalStatuses[executionId] ?: return null
        val lastBody = stepResults[executionId]?.lastOrNull()?.responseBody
        return ChildExecutionStatus(status = status, failureDescription = failureDescription, lastResultJson = lastBody)
    }

    override suspend fun getChildStatuses(executionIds: List<UUID>): Map<UUID, ChildExecutionStatus> {
        if (throwOnGetChildStatuses) error("simulated failure inside resumeAfterJoin")
        return executionIds.mapNotNull { id -> getChildStatus(id)?.let { id to it } }.toMap()
    }
}
