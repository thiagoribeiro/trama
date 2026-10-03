package run.trama.saga.workflow

import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.opentelemetry.api.trace.Span
import run.trama.saga.ExecutionOutcome
import run.trama.saga.Parking
import run.trama.saga.PersistedCheckpoint
import run.trama.saga.StaleCheckpointException
import run.trama.saga.StepRecord
import run.trama.saga.ensureLease
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureReason
import run.trama.saga.HttpCall
import run.trama.saga.HttpClientProvider
import run.trama.saga.HttpVerb
import run.trama.saga.JoinBranchLink
import run.trama.saga.PayloadValue
import run.trama.saga.RetryPolicy
import run.trama.saga.RetryState
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.StepCallEntry
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.SagaExecutor
import run.trama.runtime.JoinResumer
import run.trama.saga.TemplateContextBuilder
import run.trama.saga.TemplateEscaping
import run.trama.saga.TemplateRenderer
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.telemetry.Metrics
import run.trama.telemetry.Tracing
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
import java.time.Instant
import java.util.UUID
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Node-dispatch executor that operates on the [WorkflowDefinition] IR.
 *
 * Backward compat: understands pre-PR2 [ExecutionState.InProgress] where [activeNodeId]
 * is null, deriving the active node from [SagaExecution.currentStepIndex] + the v1 definition.
 */
class WorkflowExecutor(
    private val store: SagaExecutionStore,
    private val renderer: TemplateRenderer,
    private val retryPolicy: RetryPolicy,
    private val enqueuer: SagaEnqueuer,
    private val httpClient: HttpClientProvider,
    private val metrics: Metrics,
    private val maxNodesPerExecution: Int = 25,
    private val sleepMaxChunkMillis: Long = 12 * 3_600_000L,
    private val sleepJitterMillis: Long = 60_000L,
    callbackTokenService: CallbackTokenService? = null,
    callbackUrlFactory: CallbackUrlFactory? = null,
) : SagaExecutor, JoinResumer {
    private val logger = LoggerFactory.getLogger(WorkflowExecutor::class.java)
    private val tracer = Tracing.tracer("workflow-executor")
    private val taskHandler = TaskNodeHandler(renderer, httpClient, metrics, callbackTokenService, callbackUrlFactory)
    private val json = Json { ignoreUnknownKeys = true }

    private fun parseBodyOrNull(raw: String?): JsonElement? =
        raw?.let { runCatching { json.parseToJsonElement(it) }.getOrNull() }

    /**
     * The copy of an execution this call is advancing: its latest checkpointed state, and the seq
     * of the queue item carrying it forward (see [advance]).
     */
    private class Cursor(var execution: SagaExecution, var carrier: Long)

    override suspend fun execute(execution: SagaExecution): ExecutionOutcome {
        return Tracing.withSpan(
            tracer = tracer,
            name = "saga.execute",
            attributes = mapOf(
                "saga.id" to execution.id.toString(),
                "saga.name" to execution.definition.name,
                "saga.version" to execution.definition.version,
            ),
        ) { span ->
            if (logger.isDebugEnabled) Tracing.withTraceMdc(span, execution.id.toString()) {
                logger.debug(
                    "saga execution started",
                    kv("sagaName", execution.definition.name),
                    kv("sagaVersion", execution.definition.version),
                )
            }
            val cursor = resolveStart(execution)
            when (val state = cursor.execution.state) {
                is ExecutionState.InProgress -> {
                    val current = cursor.execution
                    val workflow = resolveWorkflow(current)
                    val activeNodeId = resolveActiveNodeId(state, current)
                    val (completed, compStack) = resolveLegacyStacks(state, current)
                    executeForward(cursor, workflow, activeNodeId, completed, compStack, state.retry)
                }
                is ExecutionState.Compensating -> {
                    val workflow = resolveWorkflow(cursor.execution)
                    executeCompensating(cursor, workflow, state)
                }
                is ExecutionState.Failed -> ExecutionOutcome.FailedFinal
                is ExecutionState.Succeeded -> {
                    // Enqueued by CallbackReceiver when the callback of a terminal async node is
                    // accepted, or adopted from a checkpoint written right before finishing:
                    // finish exactly as when the executor itself completes the last node.
                    finishSuccess(cursor, resolveWorkflow(cursor.execution), store.loadStepResults(cursor.execution.id))
                }
                is ExecutionState.WaitingCallback -> {
                    val workflow = resolveWorkflow(cursor.execution)
                    executeWaitingCallback(cursor, workflow, state)
                }
                is ExecutionState.Sleeping -> {
                    val workflow = resolveWorkflow(cursor.execution)
                    executeSleeping(cursor, workflow, state)
                }
                is ExecutionState.WaitingJoin -> recoverWaitingJoin(cursor.execution, state)
            }
        }
    }

    /**
     * Decides which copy of the execution to run, from what Postgres has recorded:
     * - same seq as this queue item: the item is current, run it;
     * - a newer seq still carried by this item: its previous worker died after checkpointing
     *   further but before handing the work on, so continue from the stored checkpoint;
     * - anything else (newer seq carried by another item, or finished): this copy is stale.
     */
    private suspend fun resolveStart(item: SagaExecution): Cursor {
        val persisted = store.readCheckpoint(item.id)
        if (persisted == null) {
            // No row: a store without durable checkpoints, or an execution started by an older
            // version that only wrote its row on parking or finishing.
            store.adoptLegacy(item)
            store.admit(listOf(item))
            return Cursor(item, item.checkpointSeq)
        }
        if (persisted.legacy) store.adoptLegacy(item)
        if (persisted.terminal) throw StaleCheckpointException(item.id)
        if (persisted.seq == item.checkpointSeq) return Cursor(item, item.checkpointSeq)
        if (persisted.seq > item.checkpointSeq && persisted.carrier == item.checkpointSeq) {
            val adopted = store.loadCheckpoint(item.id) ?: throw StaleCheckpointException(item.id)
            Tracing.withTraceMdc(Span.current(), item.id.toString()) {
                logger.info("resuming from checkpoint", kv("fromSeq", item.checkpointSeq), kv("toSeq", adopted.checkpointSeq))
            }
            return Cursor(adopted, item.checkpointSeq)
        }
        throw StaleCheckpointException(item.id)
    }

    /**
     * Makes [state] the execution's durable checkpoint (compare-and-set on its seq) together with
     * [step] and, for parking transitions, the parked state. With [handoff] the execution moves
     * on in a new queue item enqueued right after, which becomes its carrier; otherwise this call
     * keeps carrying it. Fenced: throws if this worker lost its claim or another copy got ahead.
     */
    private suspend fun advance(
        cursor: Cursor,
        state: ExecutionState,
        step: StepRecord? = null,
        parking: Parking? = null,
        handoff: Boolean = false,
        resumeAt: Instant = Instant.now(),
    ): SagaExecution {
        ensureLease()
        val next = cursor.execution.copy(state = state, checkpointSeq = cursor.execution.checkpointSeq + 1)
        val carrier = if (handoff) next.checkpointSeq else cursor.carrier
        store.checkpoint(next, carrier, resumeAt, step, parking)
        cursor.execution = next
        cursor.carrier = carrier
        return next
    }

    // ── Definition resolution ─────────────────────────────────────────────────

    /**
     * Returns the [WorkflowDefinition] for this execution.
     * Uses the v2 definition when present; falls back to normalizing the v1 definition.
     */
    private fun resolveWorkflow(execution: SagaExecution): WorkflowDefinition =
        execution.definitionV2?.let { DefinitionNormalizer.normalize(it) }
            ?: DefinitionNormalizer.normalize(execution.definition)

    // ── Forward execution ─────────────────────────────────────────────────────

    private suspend fun executeForward(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        startNodeId: String,
        initialCompleted: List<String>,
        initialCompStack: List<String>,
        initialRetry: RetryState,
    ): ExecutionOutcome {
        val execution = cursor.execution
        var activeNodeId = startNodeId
        var completedNodes = initialCompleted.toMutableList()
        var compensationStack = initialCompStack.toMutableList()
        var retry = initialRetry
        val pendingCalls = mutableListOf<StepCallEntry>()
        var processed = 0
        // Load once per execution slice; updated in-memory as each node completes so
        // subsequent nodes can reference prior results via {{nodes.X.response.body}}. Nothing to
        // load before the first node has completed (most executions run in a single slice).
        val stepResults = (if (initialCompleted.isEmpty()) emptyList() else store.loadStepResults(execution.id)).toMutableList()

        while (true) {
            val node = workflow.nodes[activeNodeId]
                ?: return handleBug(cursor.execution, "node '$activeNodeId' not found in workflow")

            val stepIdx = completedNodes.size
            // Set by nodes that complete and continue the forward walk; checkpointed below.
            val step: StepRecord

            when (node) {
                is TaskNode -> {
                    ensureLease()
                    val taskStartNanos = System.nanoTime()
                    val httpResult = taskHandler.execute(node, cursor.execution, execution.payload, stepResults)
                    recordNodeDuration(execution, "task", if (node.action.mode == TaskMode.ASYNC) "async" else "sync", taskStartNanos)

                    step = StepRecord(
                        stepIdx = stepIdx,
                        stepName = node.id,
                        phase = ExecutionPhase.UP,
                        statusCode = httpResult.statusCode,
                        // WaitingForCallback is not a failure — the trigger was accepted
                        success = httpResult.nodeResult !is NodeResult.NodeFailed,
                        responseBody = httpResult.responseBody,
                        stepStartedAt = httpResult.stepStartedAt,
                    )
                    pendingCalls += StepCallEntry(
                        sagaId = execution.id,
                        sagaStartedAt = execution.startedAt,
                        stepName = node.id,
                        phase = ExecutionPhase.UP,
                        attempt = (retry as? RetryState.Applying)?.attempt ?: 0,
                        requestUrl = httpResult.requestUrl,
                        requestBody = httpResult.requestBody,
                        statusCode = httpResult.statusCode,
                        responseBody = httpResult.responseBody,
                        error = httpResult.error,
                        stepStartedAt = httpResult.stepStartedAt,
                    )

                    when (val result = httpResult.nodeResult) {
                        is NodeResult.NodeFailed -> {
                            if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                            return handleForwardFailure(
                                cursor, workflow, activeNodeId,
                                completedNodes, compensationStack,
                                retry, result.reason, step,
                            )
                        }
                        is NodeResult.Advanced -> {
                            retry = RetryState.None
                            completedNodes.add(node.id)
                            stepResults.add(StepResult(stepIdx, node.id, parseBodyOrNull(httpResult.responseBody), null))
                            if (node.compensation != null) {
                                compensationStack.add(0, node.id)
                            }
                            if (node.next == null) {
                                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                                advance(cursor, ExecutionState.Succeeded(completedAt = Instant.now()), step)
                                return finishSuccess(cursor, workflow, stepResults)
                            }
                            activeNodeId = node.next
                        }
                        is NodeResult.WaitingForCallback -> {
                            if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                            val updated = advance(
                                cursor,
                                ExecutionState.WaitingCallback(
                                    nodeId = result.nodeId,
                                    attempt = result.attempt,
                                    deadlineAt = result.deadlineAt,
                                    nonce = result.nonce,
                                    completedNodes = completedNodes.toList(),
                                    compensationStack = compensationStack.toList(),
                                ),
                                step = step,
                                parking = Parking.Callback(result.signature),
                                handoff = true,
                                resumeAt = result.deadlineAt,
                            )
                            val delayMillis = (result.deadlineAt.toEpochMilli() - System.currentTimeMillis()).coerceAtLeast(0)
                            enqueuer.enqueue(updated, delayMillis)
                            metrics.recordCallbackWaitEntered(execution.definition.name, execution.definition.version)
                            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                                logger.info(
                                    "async node waiting for callback",
                                    kv("nodeId", result.nodeId),
                                    kv("deadlineAt", result.deadlineAt.toString()),
                                )
                            }
                            return ExecutionOutcome.Reenqueued
                        }
                    }
                }

                is SwitchNode -> {
                    val switchStartedAt = Instant.now()
                    val switchStartNanos = System.nanoTime()
                    val evalResult = SwitchNodeHandler.evaluate(node, execution, execution.payload, stepResults)
                    recordNodeDuration(execution, "switch", "none", switchStartNanos)
                    val traceJson = buildSwitchTraceJson(evalResult)
                    step = StepRecord(
                        stepIdx = stepIdx,
                        stepName = node.id,
                        phase = ExecutionPhase.SWITCH,
                        statusCode = null,
                        success = true,
                        responseBody = traceJson,
                        stepStartedAt = switchStartedAt,
                    )
                    stepResults.add(StepResult(stepIdx, node.id, parseBodyOrNull(traceJson), null))
                    retry = RetryState.None
                    activeNodeId = evalResult.targetNodeId
                    metrics.recordSwitchEvaluated(
                        sagaName = execution.definition.name,
                        sagaVersion = execution.definition.version,
                        result = if (evalResult.usedDefault) "default" else "case",
                    )
                    Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                        logger.info(
                            "switch evaluated",
                            kv("nodeId", node.id),
                            kv("target", evalResult.targetNodeId),
                            kv("matchedCase", evalResult.matchedCaseName),
                            kv("usedDefault", evalResult.usedDefault),
                        )
                    }
                }

                is SleepNode -> {
                    if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                    val wakeAt = Instant.now().plusMillis(node.durationMillis)
                    val delay = minOf(node.durationMillis, sleepMaxChunkMillis + sleepJitterMillis)
                    val sleepStartNanos = System.nanoTime()
                    val updated = advance(
                        cursor,
                        ExecutionState.Sleeping(
                            wakeAt = wakeAt,
                            nextNodeId = node.next,
                            completedNodes = completedNodes.toList(),
                            compensationStack = compensationStack.toList(),
                        ),
                        parking = Parking.Sleep(wakeAt),
                        handoff = true,
                        resumeAt = wakeAt,
                    )
                    enqueuer.enqueue(updated, delay)
                    recordNodeDuration(execution, "sleep", "none", sleepStartNanos)
                    Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                        logger.info(
                            "saga sleeping",
                            kv("nodeId", node.id),
                            kv("wakeAt", wakeAt.toString()),
                            kv("delayMillis", delay),
                        )
                    }
                    return ExecutionOutcome.Reenqueued
                }

                is SplitNode -> {
                    if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                    completedNodes.add(node.id)

                    val children = node.branches.map { branchNodeId ->
                        // Deterministic id/startedAt (not random/now()) so a redelivery of this
                        // very split step — e.g. the worker crashed mid-spawn and the parent's
                        // message got requeued — reconstructs byte-identical children instead of
                        // a brand new batch. Combined with the ON CONFLICT on saga_join_branch and
                        // the idempotent markChildArrived, this means a retried split can never
                        // corrupt the barrier's bookkeeping or spawn extra untracked children.
                        val childId = UUID.nameUUIDFromBytes("${execution.id}:${node.id}:$branchNodeId".toByteArray())
                        SagaExecution(
                            definition = execution.definition,
                            definitionV2 = execution.definitionV2,
                            id = childId,
                            startedAt = execution.startedAt,
                            currentStepIndex = 0,
                            state = ExecutionState.InProgress(activeNodeId = branchNodeId),
                            payload = execution.payload,
                            parentExecutionId = execution.id,
                            parentStartedAt = execution.startedAt,
                            parentSplitNodeId = node.id,
                            parentJoinNodeId = node.join,
                            branchId = branchNodeId,
                        )
                    }

                    // Children get their rows before they are registered on the barrier: a
                    // registered branch must always be resumable from Postgres, even if this
                    // worker dies before enqueuing it (the reconciler then picks it up).
                    val splitStartNanos = System.nanoTime()
                    store.admit(children)
                    // Barrier + parked parent state must be durable BEFORE any child can run,
                    // since a child could finish (and call markChildArrived) as soon as
                    // it is enqueued below.
                    val newlyRegisteredBranches = store.registerJoinBarrier(
                        parentId = execution.id,
                        parentStartedAt = execution.startedAt,
                        splitNodeId = node.id,
                        joinNodeId = node.join,
                        branches = children.map { child -> JoinBranchLink(child.branchId!!, child.id, child.startedAt) },
                    )
                    advance(
                        cursor,
                        ExecutionState.WaitingJoin(
                            splitNodeId = node.id,
                            joinNodeId = node.join,
                            expectedBranches = children.size,
                            completedNodes = completedNodes.toList(),
                            compensationStack = compensationStack.toList(),
                        ),
                        step = StepRecord(
                            stepIdx = stepIdx,
                            stepName = node.id,
                            phase = ExecutionPhase.SPLIT,
                            statusCode = null,
                            success = true,
                            responseBody = buildSplitTraceJson(children),
                            stepStartedAt = Instant.now(),
                        ),
                        parking = Parking.Join,
                        handoff = true,
                    )
                    // Only enqueue branches newly registered by the call above: a redelivery of
                    // this very split step reconstructs the same deterministic children, but any
                    // branch already registered by an earlier (crashed) attempt was also already
                    // enqueued then — re-enqueuing it here would run its entire subtree of work a
                    // second time, independently of whatever the first copy already did, rather
                    // than just repeating a single call the way an ordinary node redelivery does.
                    children.filter { child -> child.branchId in newlyRegisteredBranches }
                        .forEach { child -> enqueuer.enqueue(child, 0) }
                    recordNodeDuration(execution, "split", "none", splitStartNanos)

                    Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                        logger.info("saga split", kv("nodeId", node.id), kv("branchCount", children.size))
                    }
                    return ExecutionOutcome.Reenqueued
                }

                is JoinNode -> {
                    // Joins are only ever reached via the resume path built by
                    // finalizeAndNotifyParent/resumeAfterJoin (which sets activeNodeId to
                    // join.next directly) — walking into a join node here is a definition/
                    // executor bug, not a runtime condition.
                    return handleBug(cursor.execution, "join node '${node.id}' reached directly by the forward walk")
                }
            }

            // The node completed and the walk continues at activeNodeId: checkpoint it. Past
            // maxNodesPerExecution the rest of the walk is handed to a fresh queue item.
            processed++
            val handoff = processed >= maxNodesPerExecution
            val updated = advance(
                cursor,
                ExecutionState.InProgress(
                    activeNodeId = activeNodeId,
                    completedNodes = completedNodes.toList(),
                    compensationStack = compensationStack.toList(),
                ),
                step = step,
                handoff = handoff,
            )
            if (handoff) {
                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                enqueuer.enqueue(updated, 0)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.info("execution checkpoint scheduled", kv("nextNodeId", activeNodeId))
                }
                return ExecutionOutcome.Reenqueued
            }
        }
    }

    private suspend fun handleForwardFailure(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        failedNodeId: String,
        completedNodes: List<String>,
        compensationStack: List<String>,
        retryState: RetryState,
        reason: FailureReason,
        step: StepRecord,
    ): ExecutionOutcome {
        val execution = cursor.execution
        val retryDecision = retryPolicy.next(retryState, workflow.failureHandling)
        return if (retryDecision.shouldRetry) {
            Span.current().addEvent("saga.retry")
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info(
                    "retry scheduled",
                    kv("delayMillis", retryDecision.delayMillis),
                    kv("attempt", retryDecision.attempt),
                )
            }
            val updated = advance(
                cursor,
                ExecutionState.InProgress(
                    activeNodeId = failedNodeId,
                    completedNodes = completedNodes,
                    compensationStack = compensationStack,
                    retry = RetryState.Applying(retryDecision.attempt, retryDecision.delayMillis),
                ),
                step = step,
                handoff = true,
                resumeAt = Instant.now().plusMillis(retryDecision.delayMillis),
            )
            enqueuer.enqueue(updated, retryDecision.delayMillis)
            ExecutionOutcome.Reenqueued
        } else {
            Span.current().addEvent("saga.compensate")
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info("compensation scheduled", kv("failedNodeId", failedNodeId))
            }
            val updated = advance(
                cursor,
                ExecutionState.Compensating(
                    compensationStack = compensationStack,
                    completedNodes = completedNodes,
                    failureReason = reason,
                ),
                step = step,
                handoff = true,
            )
            store.updateFailure(execution.id, reason.message, null, null)
            enqueuer.enqueue(updated, 0)
            ExecutionOutcome.Reenqueued
        }
    }

    private suspend fun finishSuccess(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        stepResults: List<StepResult>,
    ): ExecutionOutcome {
        val execution = cursor.execution
        workflow.onSuccessCallback?.let { callback ->
            ensureLease()
            val context = TemplateContextBuilder.build(execution, "onSuccessCallback", ExecutionPhase.UP, stepResults, execution.payload)
            val httpResult = executeRawCall("onSuccessCallback", callback, context)
            if (!httpResult.success) {
                val warning = "success callback failed: node=onSuccessCallback status=${httpResult.statusCode ?: httpResult.error}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn("success callback failed", kv("warning", warning))
                }
            }
        }
        finalizeAndNotifyParent(execution, "SUCCEEDED")
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "SUCCEEDED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.Succeeded
    }

    // ── Compensation execution ────────────────────────────────────────────────

    private suspend fun executeCompensating(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        state: ExecutionState.Compensating,
    ): ExecutionOutcome {
        val execution = cursor.execution
        val remaining = state.compensationStack.toMutableList()
        var retry = state.retry
        var processed = 0
        val pendingCalls = mutableListOf<StepCallEntry>()
        // Load once per execution slice
        val stepResults = store.loadStepResults(execution.id)
        fun compensating(retry: RetryState = RetryState.None) = ExecutionState.Compensating(
            compensationStack = remaining.toList(),
            completedNodes = state.completedNodes,
            failureReason = state.failureReason,
            retry = retry,
        )

        while (remaining.isNotEmpty()) {
            val nodeId = remaining.first()
            val node = workflow.nodes[nodeId] as? TaskNode ?: run {
                remaining.removeFirst()
                continue
            }

            val stepIdx = state.completedNodes.size - remaining.size
            ensureLease()
            val httpResult = taskHandler.compensate(node, cursor.execution, execution.payload, stepResults)

            val step = StepRecord(
                stepIdx = stepIdx,
                stepName = node.id,
                phase = ExecutionPhase.DOWN,
                statusCode = httpResult.statusCode,
                success = httpResult.nodeResult is NodeResult.Advanced,
                responseBody = httpResult.responseBody,
                stepStartedAt = httpResult.stepStartedAt,
            )
            pendingCalls += StepCallEntry(
                sagaId = execution.id,
                sagaStartedAt = execution.startedAt,
                stepName = node.id,
                phase = ExecutionPhase.DOWN,
                attempt = (retry as? RetryState.Applying)?.attempt ?: 0,
                requestUrl = httpResult.requestUrl,
                requestBody = httpResult.requestBody,
                statusCode = httpResult.statusCode,
                responseBody = httpResult.responseBody,
                error = httpResult.error,
                stepStartedAt = httpResult.stepStartedAt,
            )

            when (val result = httpResult.nodeResult) {
                is NodeResult.NodeFailed -> {
                    val retryDecision = retryPolicy.next(retry, workflow.failureHandling)
                    if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                    return if (retryDecision.shouldRetry) {
                        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                            logger.info("compensation retry scheduled", kv("nodeId", nodeId))
                        }
                        val updated = advance(
                            cursor,
                            compensating(RetryState.Applying(retryDecision.attempt, retryDecision.delayMillis)),
                            step = step,
                            handoff = true,
                            resumeAt = Instant.now().plusMillis(retryDecision.delayMillis),
                        )
                        enqueuer.enqueue(updated, retryDecision.delayMillis)
                        ExecutionOutcome.Reenqueued
                    } else {
                        advance(cursor, compensating(), step = step)
                        val reason = result.reason.message
                        finalizeAndNotifyParent(cursor.execution, "CORRUPTED", reason)
                        metrics.recordSagaDuration(
                            sagaName = execution.definition.name,
                            sagaVersion = execution.definition.version,
                            finalStatus = "CORRUPTED",
                            startedAt = execution.startedAt,
                        )
                        ExecutionOutcome.FailedFinal
                    }
                }
                // Advanced, or WaitingForCallback (compensate() never returns it; treated as done)
                else -> {
                    retry = RetryState.None
                    remaining.removeFirst()
                }
            }

            processed++
            val handoff = processed >= maxNodesPerExecution && remaining.isNotEmpty()
            val updated = advance(cursor, compensating(), step = step, handoff = handoff)
            if (handoff) {
                if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)
                enqueuer.enqueue(updated, 0)
                return ExecutionOutcome.Reenqueued
            }
        }

        if (pendingCalls.isNotEmpty()) store.insertStepCalls(pendingCalls)

        // All compensations complete → fire failure callback then mark FAILED
        workflow.onFailureCallback?.let { callback ->
            ensureLease()
            val context = TemplateContextBuilder.build(execution, "onFailureCallback", ExecutionPhase.DOWN, stepResults, execution.payload)
            val httpResult = executeRawCall("onFailureCallback", callback, context)
            if (!httpResult.success) {
                val warning = "failure callback failed: node=onFailureCallback status=${httpResult.statusCode ?: httpResult.error}"
                store.updateCallbackWarning(execution.id, warning)
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.warn("failure callback failed", kv("warning", warning))
                }
            }
        }
        finalizeAndNotifyParent(cursor.execution, "FAILED", state.failureReason.message)
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "FAILED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.FailedFinal
    }

    // ── Waiting callback (timeout) ────────────────────────────────────────────

    private suspend fun executeWaitingCallback(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        state: ExecutionState.WaitingCallback,
    ): ExecutionOutcome {
        val execution = cursor.execution
        if (state.deadlineAt.isAfter(Instant.now())) {
            // Delivered early (clock skew or queue re-ordering) — re-schedule for deadline.
            val delayMillis = (state.deadlineAt.toEpochMilli() - System.currentTimeMillis()).coerceAtLeast(1)
            enqueuer.enqueue(execution, delayMillis)
            return ExecutionOutcome.Reenqueued
        }

        if (store.consumeWaiting(execution.id) == null && !stillParkedAs(execution, "WAITING_CALLBACK")) {
            // Callback was received and processed before this sentinel fired — nothing to do.
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info("callback timeout sentinel consumed but callback already handled", kv("nodeId", state.nodeId))
            }
            return ExecutionOutcome.Reenqueued
        }

        // Deadline passed without a callback → treat as failure. The checkpoint below is what
        // decides it: a callback accepted concurrently advances the same seq, and only one wins.
        val reason = FailureReason("callback timeout for node ${state.nodeId}")
        val step = StepRecord(
            stepIdx = state.completedNodes.size,
            stepName = state.nodeId,
            phase = ExecutionPhase.CALLBACK,
            statusCode = null,
            success = false,
            responseBody = null,
            stepStartedAt = Instant.now(),
        )
        val outcome = handleForwardFailure(
            cursor, workflow,
            failedNodeId = state.nodeId,
            completedNodes = state.completedNodes,
            compensationStack = state.compensationStack,
            retryState = RetryState.Applying(state.attempt, 0),
            reason = reason,
            step = step,
        )
        store.updateFailure(execution.id, reason.message, null, null)
        metrics.recordCallbackTimeout(execution.definition.name, execution.definition.version)
        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.warn("callback timeout", kv("nodeId", state.nodeId), kv("attempt", state.attempt))
        }
        return outcome
    }

    /**
     * True when Postgres still shows [execution] parked as [status] at exactly this copy's seq:
     * nobody advanced it. Lets a resume proceed when its parked-state marker was consumed by a
     * process that died before advancing (otherwise the execution would stay parked forever).
     */
    private suspend fun stillParkedAs(execution: SagaExecution, status: String): Boolean {
        val persisted = store.readCheckpoint(execution.id) ?: return false
        return !persisted.legacy && persisted.status == status && persisted.seq == execution.checkpointSeq
    }

    // ── Sleep execution ───────────────────────────────────────────────────────

    private suspend fun executeSleeping(
        cursor: Cursor,
        workflow: WorkflowDefinition,
        state: ExecutionState.Sleeping,
    ): ExecutionOutcome {
        val execution = cursor.execution
        val now = Instant.now()
        if (state.wakeAt.isAfter(now)) {
            // Not yet time to wake — check whether the sleep is still pending. If it's gone, the
            // wake endpoint already fired a fresh execution; this queue item is stale → discard.
            val entry = store.peekSleeping(execution.id)
            if (entry == null) {
                Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                    logger.info("stale sleeping queue item discarded (already woken)", kv("sagaId", execution.id.toString()))
                }
                return ExecutionOutcome.Reenqueued
            }
            // Re-enqueue with the next chunk delay.
            val remaining = state.wakeAt.toEpochMilli() - now.toEpochMilli()
            val delay = minOf(remaining, sleepMaxChunkMillis + sleepJitterMillis)
            enqueuer.enqueue(execution, delay)
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info(
                    "saga still sleeping, re-enqueued",
                    kv("wakeAt", state.wakeAt.toString()),
                    kv("delayMillis", delay),
                )
            }
            return ExecutionOutcome.Reenqueued
        }

        // wakeAt has passed. Consuming the sleep is what claims the wake-up: exactly one queue
        // item per sleep wins. Every other copy — the original chunk arriving after a /wake, or a
        // second concurrent /wake — finds it gone and is a stale duplicate; advancing it would run
        // the next node twice.
        if (store.consumeSleeping(execution.id) == null && !stillParkedAs(execution, "SLEEPING")) {
            Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
                logger.info("stale sleeping queue item discarded (already woken)", kv("sagaId", execution.id.toString()))
            }
            return ExecutionOutcome.Reenqueued
        }
        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.info("saga waking up", kv("nextNodeId", state.nextNodeId))
        }
        return if (state.nextNodeId == null) {
            // Sleep was the terminal node.
            advance(cursor, ExecutionState.Succeeded(completedAt = Instant.now()))
            finishSuccess(cursor, workflow, store.loadStepResults(execution.id))
        } else {
            // Back to IN_PROGRESS before running anything, so a crash from here on resumes the
            // walk instead of finding the sleep already consumed.
            advance(
                cursor,
                ExecutionState.InProgress(
                    activeNodeId = state.nextNodeId,
                    completedNodes = state.completedNodes,
                    compensationStack = state.compensationStack,
                ),
            )
            executeForward(cursor, workflow, state.nextNodeId, state.completedNodes, state.compensationStack, RetryState.None)
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun recordNodeDuration(execution: SagaExecution, nodeKind: String, mode: String, startNanos: Long) =
        metrics.recordNodeDuration(
            execution.definition.name, execution.definition.version, nodeKind, mode, System.nanoTime() - startNanos,
        )

    /**
     * Resolves the active node id from [InProgress] state.
     * Legacy (pre-PR2) executions have [InProgress.activeNodeId] == null;
     * we derive the node id from [SagaExecution.currentStepIndex].
     */
    private fun resolveActiveNodeId(state: ExecutionState.InProgress, execution: SagaExecution): String {
        if (state.activeNodeId != null) return state.activeNodeId
        val steps = execution.definition.steps
        val idx = execution.currentStepIndex.coerceIn(0, steps.lastIndex)
        return steps[idx].name
    }

    /**
     * For legacy executions, rebuilds completedNodes and compensationStack from
     * [SagaExecution.currentStepIndex] + definition steps.
     */
    private fun resolveLegacyStacks(
        state: ExecutionState.InProgress,
        execution: SagaExecution,
    ): Pair<List<String>, List<String>> {
        if (state.activeNodeId != null) {
            return state.completedNodes to state.compensationStack
        }
        val steps = execution.definition.steps
        val idx = execution.currentStepIndex.coerceIn(0, steps.lastIndex)
        val completed = steps.take(idx).map { it.name }
        val compStack = steps.take(idx).reversed().filter { it.down.url.value.isNotBlank() }.map { it.name }
        return completed to compStack
    }

    private suspend fun handleBug(execution: SagaExecution, msg: String): ExecutionOutcome {
        logger.error("workflow bug: $msg", kv("sagaId", execution.id.toString()))
        finalizeAndNotifyParent(execution, "CORRUPTED", msg)
        metrics.recordSagaDuration(
            sagaName = execution.definition.name,
            sagaVersion = execution.definition.version,
            finalStatus = "CORRUPTED",
            startedAt = execution.startedAt,
        )
        return ExecutionOutcome.FailedFinal
    }

    private fun buildSwitchTraceJson(evalResult: SwitchNodeHandler.EvaluationResult): String {
        val obj = buildJsonObject {
            put("target", evalResult.targetNodeId)
            if (evalResult.matchedCaseName != null) {
                put("matchedCase", evalResult.matchedCaseName)
            } else {
                put("matchedCase", JsonNull)
            }
            put("usedDefault", evalResult.usedDefault)
        }
        return kotlinx.serialization.json.Json.encodeToString(JsonObject.serializer(), obj)
    }

    // ── Split / join ───────────────────────────────────────────────────────────

    /**
     * Backstop entry point for [run.trama.runtime.JoinCompletionScanner]: re-checks whether
     * [parentId]'s join barrier is already satisfied and, if so, resumes it. Safe to call
     * redundantly — [consumeWaitingJoin] is atomic, so only one caller (ever) actually resumes
     * a given parent.
     */
    override suspend fun resumeJoinIfSatisfied(parentId: UUID): Boolean {
        val parent = store.consumeWaitingJoin(parentId) ?: return false
        val waitingState = parent.state as? ExecutionState.WaitingJoin ?: return false
        return resumeAfterJoinOrRestore(parent, waitingState)
    }

    /**
     * Finalizes [execution] with [status]/[failureDescription], then — if this execution is a
     * branch spawned by a split — marks its arrival on the parent's join barrier.
     * [SagaExecutionStore.markChildArrived] is idempotent per child id, so a redelivered branch
     * that already finalized once cannot double-count or re-trigger a resume: only the single
     * call for which [JoinArrival.newlyMarked] is true and the barrier is now full proceeds.
     */
    private suspend fun finalizeAndNotifyParent(
        execution: SagaExecution,
        status: String,
        failureDescription: String? = null,
    ) {
        ensureLease()
        store.finalize(execution, status, failureDescription)
        // The one INFO line per execution (per-node detail is at DEBUG).
        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.info(
                "saga finished",
                kv("sagaName", execution.definition.name),
                kv("status", status),
                kv("durationMs", java.time.Duration.between(execution.startedAt, Instant.now()).toMillis()),
            )
        }

        val parentId = execution.parentExecutionId ?: return
        val parentStartedAt = execution.parentStartedAt ?: return
        val splitNodeId = execution.parentSplitNodeId ?: return

        val arrival = store.markChildArrived(parentId, parentStartedAt, splitNodeId, execution.id) ?: return
        if (!arrival.newlyMarked || arrival.arrived < arrival.expected) return

        val parent = store.consumeWaitingJoin(parentId) ?: return
        val waitingState = parent.state as? ExecutionState.WaitingJoin ?: return
        resumeAfterJoinOrRestore(parent, waitingState)
    }

    /** Everything [resumeAfterJoin] needs to commit, once the (retryable) prep work is done. */
    private data class PreparedJoinResume(val joinNode: JoinNode, val completedNodes: List<String>, val step: StepRecord)

    /**
     * Runs [resumeAfterJoin] in two phases. Only the *preparation* phase (reading branch
     * statuses, building and persisting the join's own step result — nothing externally
     * irreversible) is covered by the catch: [consumeWaitingJoin] already deleted the parent's
     * one durable pointer before this is called, so a failure there re-saves it, turning
     * "permanently stranded in WAITING_JOIN" into "retried later" instead.
     *
     * The *commit* phase (`finishSuccess`/`enqueuer.enqueue`) runs outside the try/catch on
     * purpose: once it starts, this join has already succeeded from its own point of view. In a
     * nested split/join, `finishSuccess` recursively notifies the *outer* barrier — if that
     * later, unrelated step throws, we must not re-park an execution we already marked
     * terminal; that exception is the outer barrier's problem, not a reason to undo our own
     * completion.
     */
    private suspend fun resumeAfterJoinOrRestore(parent: SagaExecution, state: ExecutionState.WaitingJoin): Boolean {
        val prepared = try {
            prepareJoinResume(parent, state)
        } catch (ex: JoinResumeBugException) {
            // Structural bug (e.g. a stored joinNodeId no longer resolves) — not transient, so
            // retrying via saveWaitingJoin would just reproduce the same failure forever on every
            // JoinCompletionScanner pass. Finalize as CORRUPTED like every other workflow bug in
            // this executor instead of leaving the execution stuck in WAITING_JOIN indefinitely.
            handleBug(parent, ex.message ?: "join resume failed")
            return true
        } catch (ex: Exception) {
            logger.error("resumeAfterJoin failed before committing; restoring join pointer for a later retry", kv("sagaId", parent.id.toString()), ex)
            runCatching { store.saveWaitingJoin(parent) }
            return false
        }
        commitJoinResume(parent, state, prepared)
        return true
    }

    /** Marks a failure in [prepareJoinResume] as a permanent workflow bug, not a transient one. */
    private class JoinResumeBugException(message: String) : Exception(message)

    /** Retryable half of resuming a join: no irreversible side effect past this point. */
    private suspend fun prepareJoinResume(execution: SagaExecution, state: ExecutionState.WaitingJoin): PreparedJoinResume {
        val workflow = resolveWorkflow(execution)
        val joinNode = workflow.nodes[state.joinNodeId] as? JoinNode
            ?: throw JoinResumeBugException("join node '${state.joinNodeId}' not found in workflow")

        val branches = store.getJoinBranches(execution.id, execution.startedAt, state.splitNodeId)
        val statuses = store.getChildStatuses(branches.map { it.childId })
        var allSucceeded = branches.isNotEmpty()
        val branchSummaries = branches.map { link ->
            val childStatus = statuses[link.childId]
            val statusStr = childStatus?.status ?: "UNKNOWN"
            if (statusStr != "SUCCEEDED") allSucceeded = false
            buildJsonObject {
                put("branchId", link.branchId)
                put("executionId", link.childId.toString())
                put("status", statusStr)
                childStatus?.failureDescription?.let { put("failureDescription", it) }
                childStatus?.lastResultJson?.let { raw -> parseBodyOrNull(raw)?.let { put("result", it) } }
            }
        }
        val joinBodyJson = Json.encodeToString(
            JsonObject.serializer(),
            buildJsonObject {
                put("expected", state.expectedBranches)
                put("allSucceeded", allSucceeded)
                put("branches", JsonArray(branchSummaries))
            },
        )

        Tracing.withTraceMdc(Span.current(), execution.id.toString()) {
            logger.info(
                "join barrier satisfied",
                kv("splitNodeId", state.splitNodeId),
                kv("joinNodeId", state.joinNodeId),
                kv("allSucceeded", allSucceeded),
            )
        }

        val step = StepRecord(
            stepIdx = state.completedNodes.size,
            stepName = state.joinNodeId,
            phase = ExecutionPhase.JOIN,
            statusCode = null,
            success = true,
            responseBody = joinBodyJson,
            stepStartedAt = Instant.now(),
        )
        return PreparedJoinResume(joinNode, state.completedNodes + state.joinNodeId, step)
    }

    /** Point of no return: commits the join's outcome. Not covered by the restore-on-failure catch. */
    private suspend fun commitJoinResume(execution: SagaExecution, state: ExecutionState.WaitingJoin, prepared: PreparedJoinResume) {
        val workflow = resolveWorkflow(execution)
        val joinNode = prepared.joinNode
        val completedNodes = prepared.completedNodes
        val cursor = Cursor(execution, execution.checkpointSeq)
        if (joinNode.next == null) {
            advance(cursor, ExecutionState.Succeeded(completedAt = Instant.now()), step = prepared.step)
            finishSuccess(cursor, workflow, store.loadStepResults(execution.id))
            return
        }
        val updated = advance(
            cursor,
            ExecutionState.InProgress(
                activeNodeId = joinNode.next,
                completedNodes = completedNodes,
                compensationStack = state.compensationStack,
            ),
            step = prepared.step,
            handoff = true,
        )
        enqueuer.enqueue(updated, 0)
    }

    /**
     * A parent parked on a join reached the queue directly. Normally a stray redelivery of the
     * split's queue item (nothing to do: the last branch or the backstop scanner resumes it). But
     * when every branch already finished, it is a parent recovered from its checkpoint after the
     * process resuming it died mid-resume, so the join is resumed here.
     */
    private suspend fun recoverWaitingJoin(execution: SagaExecution, state: ExecutionState.WaitingJoin): ExecutionOutcome {
        val branches = store.getJoinBranches(execution.id, execution.startedAt, state.splitNodeId)
        val statuses = store.getChildStatuses(branches.map { it.childId })
        val allFinished = branches.isNotEmpty() && branches.all { statuses[it.childId]?.status in PersistedCheckpoint.TERMINAL_STATUSES }
        if (!allFinished) {
            logger.warn("unexpected WaitingJoin execution dequeued directly", kv("sagaId", execution.id.toString()))
            return ExecutionOutcome.Reenqueued
        }
        resumeAfterJoinOrRestore(execution, state)
        return ExecutionOutcome.Reenqueued
    }

    private fun buildSplitTraceJson(children: List<SagaExecution>): String {
        val arr = JsonArray(
            children.map { child ->
                buildJsonObject {
                    put("branchId", child.branchId ?: "")
                    put("executionId", child.id.toString())
                }
            },
        )
        return Json.encodeToString(JsonArray.serializer(), arr)
    }

    private data class RawCallResult(val success: Boolean, val statusCode: Int?, val error: String?)

    private suspend fun executeRawCall(
        name: String,
        call: HttpCall,
        context: Map<String, Any?>,
    ): RawCallResult {
        val url = renderer.render(call.url, context, TemplateEscaping.NONE)
        return try {
            val response = httpClient.client.request(url) {
                method = call.verb.toKtorMethod()
                call.headers.forEach { (k, v) -> header(k, renderer.render(v, context, TemplateEscaping.HEADER_VALUE)) }
                call.body?.let { setBody(renderer.render(it, context, TemplateEscaping.forBody(call))) }
            }
            RawCallResult(
                success = response.status.value in call.successStatusCodes,
                statusCode = response.status.value,
                error = null,
            )
        } catch (ex: Exception) {
            RawCallResult(success = false, statusCode = null, error = ex.message)
        }
    }

}
