package run.trama.cli

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.toAny
import run.trama.saga.workflow.DefinitionNormalizer
import run.trama.saga.workflow.JoinNode
import run.trama.saga.workflow.JsonLogicEvaluator
import run.trama.saga.workflow.SleepNode
import run.trama.saga.workflow.SplitNode
import run.trama.saga.workflow.SwitchNode
import run.trama.saga.workflow.TaskNode
import run.trama.saga.workflow.WorkflowDefinition

// ─── Scenario ────────────────────────────────────────────────────────────────

/**
 * Mock response for a single node.
 *
 * Sync nodes:  [status] + [body]
 * Async nodes: [acceptedStatus] + [callbackBody]  (the orchestrator pauses until callback fires)
 *
 * Any node not listed in the scenario defaults to status=200, body={}
 */
@Serializable
data class StepMock(
    val status: Int = 200,
    val body: JsonElement = JsonObject(emptyMap()),
    val acceptedStatus: Int = 202,
    val callbackBody: JsonElement = JsonObject(emptyMap()),
)

/**
 * Full test scenario for a dry-run.
 *
 * Example scenario.json:
 * ```json
 * {
 *   "payload": { "orderId": "test-001", "paymentMethod": "pix" },
 *   "steps": {
 *     "validate":    { "status": 200, "body": { "valid": true } },
 *     "pix-payment": { "status": 200, "body": { "charged": true } },
 *     "card-payment":{ "acceptedStatus": 202, "callbackBody": { "status": "approved" } },
 *     "notify":      { "status": 200, "body": { "notified": true } }
 *   }
 * }
 * ```
 */
@Serializable
data class DryRunScenario(
    val payload: JsonObject = JsonObject(emptyMap()),
    val steps: Map<String, StepMock> = emptyMap(),
)

// ─── Trace ────────────────────────────────────────────────────────────────────

sealed class TraceEntry {
    /** A sync task node execution. */
    data class Task(
        val nodeId: String,
        val verb: String,
        val renderedUrl: String,
        val renderedBody: String?,
        val responseStatus: Int,
        val responseBody: String,
        val success: Boolean,
    ) : TraceEntry()

    /** An async task node: accepted immediately, callback body provided by scenario. */
    data class AsyncTask(
        val nodeId: String,
        val verb: String,
        val renderedUrl: String,
        val renderedBody: String?,
        val callbackBody: String,
    ) : TraceEntry()

    /** A switch node with its routing outcome. */
    data class Switch(
        val nodeId: String,
        val matchedCase: String?,
        val usedDefault: Boolean,
        val targetNodeId: String,
    ) : TraceEntry()

    /** A split node: fans out into independent branches, each simulated from a clean context. */
    data class Split(
        val nodeId: String,
        val branches: Map<String, List<TraceEntry>>,
    ) : TraceEntry()

    /** A join node: the barrier point where all branches of the owning split converged. */
    data class Join(
        val nodeId: String,
        val allSucceeded: Boolean,
    ) : TraceEntry()
}

enum class SimOutcome { SUCCEEDED, FAILED, MAX_NODES_EXCEEDED, CYCLE_DETECTED }

data class SimulationResult(
    val entries: List<TraceEntry>,
    val outcome: SimOutcome,
    val failureNodeId: String? = null,
    val failureStatus: Int? = null,
)

// ─── Simulator ────────────────────────────────────────────────────────────────

/**
 * Walks a [SagaDefinitionV2] node-graph offline, using mock responses from [DryRunScenario].
 *
 * Reuses the real engine components:
 *  - [MustacheTemplateRenderer] for template substitution
 *  - [JsonLogicEvaluator]       for switch conditions
 *  - [DefinitionNormalizer]     for V2 → WorkflowDefinition IR
 *
 * No network calls are made. No Redis or Postgres are required.
 */
class DryRunSimulator {

    private val renderer = MustacheTemplateRenderer()

    fun run(definition: SagaDefinitionV2, scenario: DryRunScenario): SimulationResult {
        val workflow = DefinitionNormalizer.normalize(definition)
        val payload = scenario.payload.toMap() // Map<String, JsonElement>
        return walk(workflow, definition, payload, scenario, workflow.entrypoint, mutableSetOf(), mutableListOf())
    }

    /**
     * Walks the graph from [startId], mutating [entries]/[stepResults] in place until a
     * terminal node, a failure, a cycle, or the node-count cap is reached. Used both for
     * the main trunk and, recursively, for each branch of a split (with a fresh
     * [visited]/[stepResults] scope, since a branch is an independent child execution
     * and does not see the parent's prior step results).
     */
    private fun walk(
        workflow: WorkflowDefinition,
        definition: SagaDefinitionV2,
        payload: Map<String, JsonElement>,
        scenario: DryRunScenario,
        startId: String,
        visited: MutableSet<String>,
        stepResults: MutableList<StepResult>,
        entries: MutableList<TraceEntry> = mutableListOf(),
    ): SimulationResult {
        var currentId: String? = startId
        var stepIndex = stepResults.size

        while (currentId != null) {
            if (currentId in visited) return SimulationResult(entries, SimOutcome.CYCLE_DETECTED)
            if (stepIndex > 50) return SimulationResult(entries, SimOutcome.MAX_NODES_EXCEEDED)
            visited += currentId

            val node = workflow.nodes[currentId]
                ?: return SimulationResult(entries, SimOutcome.FAILED, currentId)

            when (node) {
                is TaskNode -> {
                    val isAsync = node.action.mode == TaskMode.ASYNC
                    val ctx     = buildContext(definition, payload, stepResults, node.id, isAsync)
                    val url     = renderer.render(node.action.request.url, ctx)
                    val body    = node.action.request.body?.let { renderer.render(it, ctx) }
                    val verb    = node.action.request.verb.name
                    val mock    = scenario.steps[node.id] ?: StepMock()

                    if (isAsync) {
                        val cbStr = Json.encodeToString(JsonElement.serializer(), mock.callbackBody)
                        entries += TraceEntry.AsyncTask(node.id, verb, url, body, cbStr)
                        // The callback body becomes this node's step result for downstream templates/conditions.
                        stepResults += StepResult(stepIndex, node.id, mock.callbackBody, null)
                        currentId = node.next
                    } else {
                        val respStr = Json.encodeToString(JsonElement.serializer(), mock.body)
                        val ok      = mock.status in node.action.request.successStatusCodes
                        entries += TraceEntry.Task(node.id, verb, url, body, mock.status, respStr, ok)
                        if (!ok) return SimulationResult(entries, SimOutcome.FAILED, node.id, mock.status)
                        stepResults += StepResult(stepIndex, node.id, mock.body, null)
                        currentId = node.next
                    }
                    stepIndex++
                }

                is SwitchNode -> {
                    val ctx     = buildSwitchContext(payload, stepResults)
                    val matched = node.cases.firstOrNull { JsonLogicEvaluator.evaluateBool(it.whenExpression, ctx) }
                    val target  = matched?.target ?: node.defaultTarget
                    entries += TraceEntry.Switch(node.id, matched?.name, matched == null, target)
                    currentId = target
                }
                is SleepNode -> {
                    // In a dry-run simulation, sleep nodes are skipped (no real wall-clock wait).
                    currentId = node.next
                }

                is SplitNode -> {
                    val branchTraces = linkedMapOf<String, List<TraceEntry>>()
                    var allSucceeded = true
                    var firstFailure: SimulationResult? = null
                    for (branchId in node.branches) {
                        // Each branch is an independent child execution: fresh visited set and
                        // step-result history, seeded only by the parent's payload.
                        val branchResult = walk(workflow, definition, payload, scenario, branchId, mutableSetOf(), mutableListOf())
                        branchTraces[branchId] = branchResult.entries
                        if (branchResult.outcome != SimOutcome.SUCCEEDED) {
                            allSucceeded = false
                            if (firstFailure == null) firstFailure = branchResult
                        }
                    }
                    entries += TraceEntry.Split(node.id, branchTraces)
                    if (!allSucceeded) {
                        val failure = requireNotNull(firstFailure)
                        return SimulationResult(entries, failure.outcome, failure.failureNodeId, failure.failureStatus)
                    }
                    val joinNode = workflow.nodes[node.join] as? JoinNode
                        ?: return SimulationResult(entries, SimOutcome.FAILED, node.join)
                    entries += TraceEntry.Join(joinNode.id, allSucceeded = true)
                    stepResults += StepResult(stepIndex, joinNode.id, null, null)
                    stepIndex++
                    currentId = joinNode.next
                }

                is JoinNode -> {
                    // Reached only if a join is walked directly (should not normally happen —
                    // joins are consumed by the owning SplitNode branch above).
                    currentId = node.next
                }
            }
        }

        return SimulationResult(entries, SimOutcome.SUCCEEDED)
    }

    // ── Context builders ──────────────────────────────────────────────────────

    /**
     * Mirrors [run.trama.saga.TemplateContextBuilder] without requiring a [run.trama.saga.SagaExecution].
     * Produces the same variable structure so templates behave identically to production.
     */
    private fun buildContext(
        definition: SagaDefinitionV2,
        payload: Map<String, JsonElement>,
        stepResults: List<StepResult>,
        nodeId: String,
        isAsync: Boolean,
    ): Map<String, Any?> {
        val payloadAny = payload.mapValues { it.value.toAny() }
        val nodesMap   = stepResults.associate { s ->
            s.name to mapOf("response" to mapOf("body" to (s.upBody ?: s.downBody).toAny()))
        }
        val stepsList  = stepResults.map { s ->
            mapOf(
                "index" to s.index,
                "name"  to s.name,
                "body"  to (s.upBody ?: s.downBody).toAny(),
                "up"    to mapOf("body" to s.upBody.toAny()),
                "down"  to mapOf("body" to s.downBody.toAny()),
            )
        }
        val ctx = mutableMapOf<String, Any?>(
            "sagaId"      to "<dry-run>",
            "sagaName"    to definition.name,
            "sagaVersion" to definition.version,
            "stepName"    to nodeId,
            "phase"       to "UP",
            "payload"     to payloadAny,
            "input"       to payloadAny,   // alias used by switch conditions
            "nodes"       to nodesMap,
            "steps"       to stepsList,
        )
        if (isAsync) {
            // Placeholders — in production the orchestrator injects real values here.
            ctx["runtime"] = mapOf(
                "callback" to mapOf(
                    "url"   to "[callback-url]",
                    "token" to "[callback-token]",
                )
            )
        }
        return ctx
    }

    /**
     * Context used by switch node json-logic conditions.
     * Mirrors [run.trama.saga.workflow.SwitchNodeHandler.buildEvaluationContext].
     */
    private fun buildSwitchContext(
        payload: Map<String, JsonElement>,
        stepResults: List<StepResult>,
    ): Map<String, Any?> = mapOf(
        "input" to payload.mapValues { it.value.toAny() },
        "nodes" to stepResults.associate { s ->
            s.name to mapOf("response" to mapOf("body" to (s.upBody ?: s.downBody).toAny()))
        },
    )
}
