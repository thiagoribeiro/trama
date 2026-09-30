package run.trama.saga

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.util.UUID
import run.trama.saga.workflow.WorkflowExecutor
import run.trama.telemetry.Metrics

/**
 * Shared test doubles for executor-level unit tests. Older test files keep their own private
 * fakes; new tests should use these so each file only overrides the behavior it cares about.
 */

class TestHttpClientProvider(override val client: HttpClient) : HttpClientProvider

/** Wraps a [MockEngine] handler and records every request it sees. */
class RecordingHttp(
    handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData,
) {
    val requests = mutableListOf<HttpRequestData>()
    val provider = TestHttpClientProvider(
        HttpClient(MockEngine { req ->
            requests += req
            handler(req)
        })
    )
}

class RecordingEnqueuer : SagaEnqueuer {
    data class Enqueued(val execution: SagaExecution, val delayMillis: Long)

    val enqueued = mutableListOf<Enqueued>()
    override suspend fun enqueue(execution: SagaExecution, delayMillis: Long) {
        enqueued += Enqueued(execution, delayMillis)
    }
}

/**
 * In-memory [SagaExecutionStore] that records every write. Sleep/waiting sentinels are kept
 * in maps so peek/consume semantics mirror the Redis store (consume is read-and-delete).
 */
open class RecordingStore : SagaExecutionStore {
    data class RecordedStep(
        val stepIdx: Int,
        val stepName: String,
        val phase: ExecutionPhase,
        val statusCode: Int?,
        val success: Boolean,
        val responseBody: String?,
    )

    var finalStatus: String? = null
    var finalFailureDescription: String? = null
    val failures = mutableListOf<String>()
    val callbackWarnings = mutableListOf<String>()
    val upserts = mutableListOf<SagaExecution>()
    val stepResults = mutableListOf<RecordedStep>()
    val stepCalls = mutableListOf<StepCallEntry>()
    val sleeping = mutableMapOf<UUID, SleepEntry>()
    val statusUpdates = mutableListOf<String>()
    var preloadedStepResults: List<StepResult> = emptyList()

    override suspend fun upsertStart(execution: SagaExecution) { upserts += execution }
    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        finalStatus = status
        finalFailureDescription = failureDescription
    }
    override suspend fun updateFailure(executionId: UUID, failureDescription: String, failedStepIndex: Int?, failedPhase: ExecutionPhase?) {
        failures += failureDescription
    }
    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) { callbackWarnings += warning }
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
        stepResults += RecordedStep(stepIdx, stepName, phase, statusCode, success, responseBody)
    }
    override suspend fun insertStepCalls(calls: List<StepCallEntry>) { stepCalls += calls }
    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> = preloadedStepResults
    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {}
    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? = null
    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean = true
    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: Instant) {
        sleeping[execution.id] = SleepEntry(wakeAt, execution)
    }
    override suspend fun peekSleeping(executionId: UUID): SleepEntry? = sleeping[executionId]
    override suspend fun consumeSleeping(executionId: UUID): SleepEntry? = sleeping.remove(executionId)
    override suspend fun updateStatus(executionId: UUID, status: String) { statusUpdates += status }
    override suspend fun registerJoinBarrier(parentId: UUID, parentStartedAt: Instant, splitNodeId: String, joinNodeId: String, branches: List<JoinBranchLink>): Set<String> =
        branches.map { it.branchId }.toSet()
    override suspend fun markChildArrived(parentId: UUID, parentStartedAt: Instant, splitNodeId: String, childId: UUID): JoinArrival? = null
    override suspend fun getJoinBranches(parentId: UUID, parentStartedAt: Instant, splitNodeId: String): List<JoinBranchLink> = emptyList()
    override suspend fun saveWaitingJoin(execution: SagaExecution) {}
    override suspend fun consumeWaitingJoin(executionId: UUID): SagaExecution? = null
    override suspend fun getChildStatus(executionId: UUID): ChildExecutionStatus? = null
    override suspend fun getChildStatuses(executionIds: List<UUID>): Map<UUID, ChildExecutionStatus> = emptyMap()
}

fun testExecutor(
    store: SagaExecutionStore,
    enqueuer: SagaEnqueuer,
    http: HttpClientProvider,
    maxNodesPerExecution: Int = 25,
    sleepMaxChunkMillis: Long = 12 * 3_600_000L,
    sleepJitterMillis: Long = 60_000L,
): WorkflowExecutor = WorkflowExecutor(
    store = store,
    renderer = MustacheTemplateRenderer(),
    retryPolicy = DefaultRetryPolicy(),
    enqueuer = enqueuer,
    httpClient = http,
    metrics = Metrics(SimpleMeterRegistry()),
    maxNodesPerExecution = maxNodesPerExecution,
    sleepMaxChunkMillis = sleepMaxChunkMillis,
    sleepJitterMillis = sleepJitterMillis,
)

/** Builds a fresh execution for a v2 definition, positioned at its entrypoint. */
fun v2Execution(
    def: SagaDefinitionV2,
    payload: Map<String, PayloadValue> = emptyMap(),
    state: ExecutionState = ExecutionState.InProgress(activeNodeId = def.entrypoint),
): SagaExecution = SagaExecution(
    definition = SagaDefinition(def.name, def.version, def.failureHandling, steps = emptyList()),
    definitionV2 = def,
    id = UUID.randomUUID(),
    startedAt = Instant.now(),
    currentStepIndex = 0,
    state = state,
    payload = payload,
)

fun httpCall(url: String, verb: HttpVerb = HttpVerb.POST, body: String? = null, headers: Map<String, String> = emptyMap()) =
    HttpCall(
        url = TemplateString(url),
        verb = verb,
        headers = headers.mapValues { TemplateString(it.value) },
        body = body?.let { TemplateString(it) },
    )

fun syncTask(id: String, url: String, next: String? = null, compensation: HttpCall? = null) =
    NodeDefinition.Task(
        id = id,
        action = NodeActionDef(mode = TaskMode.SYNC, request = httpCall(url)),
        compensation = compensation,
        next = next,
    )
