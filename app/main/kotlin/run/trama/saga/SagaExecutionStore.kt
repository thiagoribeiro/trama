package run.trama.saga

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID

/**
 * A single HTTP call attempt for a task node — buffered in memory during execution
 * and batch-flushed to [SagaExecutionStore.insertStepCalls] at each execution slice boundary.
 */
data class StepCallEntry(
    val sagaId: UUID,
    val sagaStartedAt: Instant,
    val stepName: String,
    val phase: ExecutionPhase,
    val attempt: Int,
    val requestUrl: String?,
    val requestBody: String?,
    val statusCode: Int?,
    val responseBody: String?,
    val error: String?,
    val stepStartedAt: Instant,
)

/**
 * Result of a single step execution, used for template rendering and switch evaluation.
 */
data class StepResult(
    val index: Int,
    val name: String,
    val upBody: JsonElement?,
    val downBody: JsonElement?,
)

/**
 * Persisted state for a sleeping execution, used to reconstruct the execution on wake
 * and as a sentinel to detect stale queue items.
 */
data class SleepEntry(
    val wakeAt: Instant,
    val execution: SagaExecution,
)

/** One branch's linkage to its spawned child execution, recorded when a split fires. */
data class JoinBranchLink(
    val branchId: String,
    val childId: UUID,
    val childStartedAt: Instant,
)

/**
 * Result of marking a child's arrival on its parent's join barrier.
 * [newlyMarked] is false when this exact child id was already recorded as arrived (a redelivery
 * of an already-finalized branch) — callers must only act as "the winner" when newlyMarked is
 * true AND [arrived] == [expected]; otherwise the arrival was a no-op replay.
 */
data class JoinArrival(
    val arrived: Int,
    val expected: Int,
    val newlyMarked: Boolean,
)

/** Minimal terminal-status summary for a (already finalized) child execution. */
data class ChildExecutionStatus(
    val status: String,
    val failureDescription: String?,
    /** Raw JSON of the child's last recorded step result, if any. */
    val lastResultJson: String?,
)

/**
 * Minimal info about a waiting execution, used during callback validation and timeout processing.
 */
data class WaitingInfo(
    val nodeId: String,
    val attempt: Int,
    val nonce: String,
    val signature: String,
    val expiresAt: Instant,
    /** Full execution with [ExecutionState.WaitingCallback] state, for re-enqueueing after callback. */
    val execution: SagaExecution,
)

interface SagaExecutionStore {
    suspend fun upsertStart(execution: SagaExecution)
    suspend fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String? = null)
    suspend fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    )
    suspend fun updateCallbackWarning(executionId: java.util.UUID, warning: String)
    suspend fun insertStepResult(
        sagaId: java.util.UUID,
        startedAt: java.time.Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: java.time.Instant? = null,
    )
    suspend fun insertStepCalls(calls: List<StepCallEntry>)
    suspend fun loadStepResults(sagaId: java.util.UUID): List<StepResult>

    /**
     * Persists a [WaitingInfo] entry for the given execution so the callback receiver
     * can validate tokens and re-enqueue on valid callback.
     * [execution] must have [ExecutionState.WaitingCallback] state.
     */
    suspend fun saveWaiting(execution: SagaExecution, signature: String)

    /**
     * Atomically loads and deletes the waiting entry for [executionId].
     * Returns null when no waiting entry exists (callback already processed or never stored).
     */
    suspend fun consumeWaiting(executionId: java.util.UUID): WaitingInfo?

    /**
     * Claims [nonce] for replay protection.
     * Returns true if the nonce is fresh (first time seen); false if it was already consumed (replay).
     * [ttlSeconds] controls how long the nonce is retained.
     */
    suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean

    /**
     * Persists the sleeping state so the wake endpoint can reconstruct the execution
     * and stale queue items can detect they have been superseded.
     * TTL is set to [wakeAt] + a buffer.
     */
    suspend fun saveSleeping(execution: SagaExecution, wakeAt: Instant)

    /**
     * Returns the [SleepEntry] for [executionId] without consuming it, or null if absent.
     */
    suspend fun peekSleeping(executionId: java.util.UUID): SleepEntry?

    /**
     * Atomically reads and deletes the sleep entry for [executionId].
     * Returns null if no entry exists.
     */
    suspend fun consumeSleeping(executionId: java.util.UUID): SleepEntry?

    /**
     * Updates the status column in Postgres without finalising the execution.
     * Used to surface SLEEPING status to the status API while the saga is in the queue.
     */
    suspend fun updateStatus(executionId: java.util.UUID, status: String)

    // ── Split / join ───────────────────────────────────────────────────────────

    /**
     * Registers the join barrier for a split: how many branches are expected and which
     * child execution belongs to which branch. Must be called once, before any of the
     * spawned children can possibly finish.
     *
     * Returns the [JoinBranchLink.branchId]s that were newly registered by *this* call, as
     * opposed to ones a prior (crashed/redelivered) attempt at the same split step already
     * registered. The split node handler must enqueue only these: a redelivered split step
     * reconstructs the exact same branch set (deterministic ids), so a branch that is not
     * newly registered here was already enqueued by the earlier attempt — enqueuing it again
     * would cause its entire subtree of work to run a second time independently, not just
     * repeat a single call the way an ordinary node redelivery does.
     */
    suspend fun registerJoinBarrier(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        joinNodeId: String,
        branches: List<JoinBranchLink>,
    ): Set<String>

    /**
     * Idempotently marks [childId] as arrived on the ([parentId], [splitNodeId]) barrier and
     * returns the counts *after* this call, or null if no barrier is registered (bug/race).
     * Calling this again with a [childId] that was already marked arrived (e.g. a redelivered
     * branch re-finalizing) is a safe no-op: the counter is not incremented twice, and
     * [JoinArrival.newlyMarked] comes back false. Only proceed as "the winner" when
     * [JoinArrival.newlyMarked] is true AND [JoinArrival.arrived] == [JoinArrival.expected].
     */
    suspend fun markChildArrived(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        childId: java.util.UUID,
    ): JoinArrival?

    /** Returns the branch → child links registered by [registerJoinBarrier]. */
    suspend fun getJoinBranches(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
    ): List<JoinBranchLink>

    /**
     * Persists [execution] (state = [ExecutionState.WaitingJoin]) so [consumeWaitingJoin] can
     * retrieve it once the join barrier is satisfied.
     */
    suspend fun saveWaitingJoin(execution: SagaExecution)

    /**
     * Atomically loads and deletes the join-waiting entry for [executionId].
     * Returns null if none exists (already consumed, or never stored).
     */
    suspend fun consumeWaitingJoin(executionId: java.util.UUID): SagaExecution?

    /** Minimal terminal-status lookup for a (possibly already-finalized) child execution. */
    suspend fun getChildStatus(executionId: java.util.UUID): ChildExecutionStatus?

    /** Batched form of [getChildStatus] — one round trip regardless of how many ids are passed. */
    suspend fun getChildStatuses(executionIds: List<java.util.UUID>): Map<java.util.UUID, ChildExecutionStatus>
}

class SagaRepositoryStore(
    private val repository: SagaRepository,
) : SagaExecutionStore {
    override suspend fun upsertStart(execution: SagaExecution) =
        repository.upsertExecutionStart(execution)

    override suspend fun updateFinal(executionId: java.util.UUID, status: String, failureDescription: String?) =
        repository.updateExecutionFinal(executionId, status, failureDescription)

    override suspend fun updateFailure(
        executionId: java.util.UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) = repository.updateFailureDescription(executionId, failureDescription, failedStepIndex, failedPhase)

    override suspend fun updateCallbackWarning(executionId: java.util.UUID, warning: String) =
        repository.updateCallbackWarning(executionId, warning)

    override suspend fun insertStepResult(
        sagaId: java.util.UUID,
        startedAt: java.time.Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: java.time.Instant?,
    ) = repository.insertStepResult(
        sagaId = sagaId,
        startedAt = startedAt,
        stepIdx = stepIdx,
        stepNameValue = stepName,
        phase = phase,
        statusCode = statusCode,
        success = success,
        responseBody = responseBody,
        stepStartedAt = stepStartedAt,
    )

    override suspend fun insertStepCalls(calls: List<StepCallEntry>) =
        repository.insertStepCalls(calls)

    override suspend fun loadStepResults(sagaId: java.util.UUID): List<StepResult> =
        repository.loadStepResultsForTemplate(sagaId)

    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {
        val state = execution.state as? ExecutionState.WaitingCallback ?: return
        repository.saveWaitingState(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = Json.encodeToString(SagaExecution.serializer(), execution),
        )
    }

    override suspend fun consumeWaiting(executionId: java.util.UUID): WaitingInfo? =
        repository.consumeWaitingState(executionId)

    // Postgres path does not support distributed nonce dedup; always returns true (fresh).
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true

    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: Instant) {
        // Postgres-only path: update status so the API reflects SLEEPING.
        // The execution stays in the queue payload; no separate Redis key here.
        repository.updateStatus(execution.id, "SLEEPING")
    }

    override suspend fun peekSleeping(executionId: java.util.UUID): SleepEntry? = null

    override suspend fun consumeSleeping(executionId: java.util.UUID): SleepEntry? = null

    override suspend fun updateStatus(executionId: java.util.UUID, status: String) =
        repository.updateStatus(executionId, status)

    override suspend fun registerJoinBarrier(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        joinNodeId: String,
        branches: List<JoinBranchLink>,
    ) = repository.registerJoinBarrier(parentId, parentStartedAt, splitNodeId, joinNodeId, branches)

    override suspend fun markChildArrived(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        childId: java.util.UUID,
    ): JoinArrival? = repository.markChildArrived(parentId, parentStartedAt, splitNodeId, childId)

    override suspend fun getJoinBranches(
        parentId: java.util.UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
    ): List<JoinBranchLink> = repository.getJoinBranches(parentId, parentStartedAt, splitNodeId)

    override suspend fun saveWaitingJoin(execution: SagaExecution) {
        val state = execution.state as? ExecutionState.WaitingJoin ?: return
        repository.saveWaitingJoinState(
            executionId = execution.id,
            splitNodeId = state.splitNodeId,
            joinNodeId = state.joinNodeId,
            executionJson = Json.encodeToString(SagaExecution.serializer(), execution),
        )
    }

    override suspend fun consumeWaitingJoin(executionId: java.util.UUID): SagaExecution? =
        repository.consumeWaitingJoinState(executionId)

    override suspend fun getChildStatus(executionId: java.util.UUID): ChildExecutionStatus? =
        repository.getChildStatus(executionId)

    override suspend fun getChildStatuses(executionIds: List<java.util.UUID>): Map<java.util.UUID, ChildExecutionStatus> =
        repository.getChildStatuses(executionIds)
}
