package run.trama.saga.workflow

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.HttpCall
import run.trama.saga.PayloadValue
import run.trama.saga.RESERVED_NODES_KEY
import run.trama.saga.StepResult

/**
 * Which of the parent's node results a split hands to its branches under
 * `payload._nodes`: every one ([all]) or only those named in [names].
 */
data class BranchNodeSelection(val all: Boolean, val names: Set<String>) {
    fun includes(name: String): Boolean = all || name in names

    companion object {
        val ALL = BranchNodeSelection(all = true, names = emptySet())
    }
}

/**
 * Builds the initial payload of a split's branches: the parent's payload plus [RESERVED_NODES_KEY],
 * an object mapping each node the parent ran before the split to its response body — the same
 * value `nodes.<node>.response.body` resolves to in the parent. The branch's own step results
 * start empty, as before.
 */
object BranchPayload {

    /**
     * [stepResults] are the parent's results so far, in execution order. Records of split nodes
     * (the fan-out trace, not a node result) are left out; joins stay, as they are the result of a
     * node. Inherited `_nodes` (the parent is itself a branch) are merged in first, so on a name
     * clash the immediate parent wins. Only the nodes the split's branches reference are kept
     * (see [SplitNodeReferences]). Pure function of its inputs: a redelivered split rebuilds
     * byte-identical children.
     */
    fun build(
        parentPayload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
        workflow: WorkflowDefinition,
        split: SplitNode,
    ): Map<String, PayloadValue> {
        val selection = workflow.branchNodeSelection(split.id)
        val nodes = linkedMapOf<String, JsonElement>()
        (parentPayload[RESERVED_NODES_KEY]?.value as? JsonObject)?.forEach { (name, body) -> nodes[name] = body }
        for (step in stepResults) {
            if (workflow.nodes[step.name] is SplitNode) continue
            nodes.remove(step.name) // re-insert so the order follows the parent's own results
            nodes[step.name] = step.upBody ?: step.downBody ?: JsonNull
        }
        val selected = JsonObject(nodes.filterKeys { selection.includes(it) })
        return parentPayload + (RESERVED_NODES_KEY to PayloadValue(selected))
    }
}

/**
 * Static analysis of which parent node results each split's branches can read through
 * `payload._nodes.<node>` (or its `input._nodes.<node>` alias). Computed once per
 * [WorkflowDefinition] (cached with it, see [WorkflowDefinition.branchNodeSelection]).
 *
 * The scanned subgraph of a split runs from each branch's first node up to (not including) the
 * split's join, so nested splits — their branches and whatever follows their joins — are part of
 * it and their needs propagate up. The workflow-level callbacks are scanned too, since a branch
 * fires them when it finishes.
 *
 * Anything that cannot be resolved statically makes the split carry every node (the safe
 * fallback): a bare `payload._nodes`, a `_nodes` outside a `payload.`/`input.` path (e.g. inside a
 * `{{#payload}}` section), a tag over the whole payload (`{{payload}}`, `{{#input}}`, `{{.}}`), a
 * json-logic `var` whose path is not a literal string or reads the whole payload.
 */
object SplitNodeReferences {
    private val nodesToken = Regex("""(?<![A-Za-z0-9_.])(?:(payload|input)\.)?_nodes(?:\.([^.\s{}\[\]"'\\|]+))?""")
    // Mustache tag (plain, section, inverted, unescaped, closing) over the whole payload or context.
    private val wholePayloadTag = Regex("""\{\{\s*[#^&{/]?\s*(?:payload|input|\.)\s*}""")
    private val wholePayloadVars = setOf("", "payload", "input")

    fun analyze(workflow: WorkflowDefinition): Map<String, BranchNodeSelection> =
        workflow.nodes.values.filterIsInstance<SplitNode>().associate { split -> split.id to analyzeSplit(workflow, split) }

    private fun analyzeSplit(workflow: WorkflowDefinition, split: SplitNode): BranchNodeSelection {
        val acc = Accumulator()
        workflow.onSuccessCallback?.let { acc.scanCall(it) }
        workflow.onFailureCallback?.let { acc.scanCall(it) }
        val visited = mutableSetOf<String>()
        val queue = ArrayDeque(split.branches)
        while (queue.isNotEmpty() && !acc.all) {
            val id = queue.removeFirst()
            if (id == split.join || !visited.add(id)) continue
            when (val node = workflow.nodes[id] ?: continue) {
                is TaskNode -> {
                    acc.scanCall(node.action.request)
                    node.compensation?.let { acc.scanCall(it) }
                    node.action.callbackConfig?.successWhen?.let { acc.scanLogic(it) }
                    node.action.callbackConfig?.failureWhen?.let { acc.scanLogic(it) }
                    node.next?.let { queue += it }
                }
                is SwitchNode -> {
                    node.cases.forEach { case ->
                        acc.scanLogic(case.whenExpression)
                        queue += case.target
                    }
                    queue += node.defaultTarget
                }
                is SleepNode -> node.next?.let { queue += it }
                is SplitNode -> {
                    queue += node.branches
                    queue += node.join
                }
                is JoinNode -> node.next?.let { queue += it }
            }
        }
        return if (acc.all) BranchNodeSelection.ALL else BranchNodeSelection(all = false, names = acc.names.toSet())
    }

    private class Accumulator {
        var all = false
        val names = linkedSetOf<String>()

        fun scanCall(call: HttpCall) {
            scanText(call.url.value)
            call.headers.values.forEach { scanText(it.value) }
            call.body?.let { scanText(it.value) }
        }

        fun scanText(text: String) {
            if (wholePayloadTag.containsMatchIn(text)) {
                all = true
                return
            }
            scanTokens(text)
        }

        private fun scanTokens(text: String) {
            for (match in nodesToken.findAll(text)) {
                val root = match.groups[1]
                val name = match.groups[2]
                if (root == null || name == null) {
                    all = true
                    return
                }
                names += name.value
            }
        }

        fun scanLogic(element: JsonElement) {
            when (element) {
                is JsonObject -> element.forEach { (op, arg) ->
                    if (op == "var") scanVar(arg)
                    scanLogic(arg)
                }
                is JsonArray -> element.forEach { scanLogic(it) }
                is JsonPrimitive -> if (element.isString) scanTokens(element.content)
            }
        }

        private fun scanVar(arg: JsonElement) {
            val path = when (arg) {
                is JsonArray -> arg.firstOrNull() ?: JsonNull
                else -> arg
            }
            if (path is JsonNull) {
                all = true // {"var": null} / {"var": []} read the whole data
                return
            }
            if (path !is JsonPrimitive) {
                all = true // dynamic path, built at evaluation time
                return
            }
            if (path.content in wholePayloadVars) all = true
        }
    }
}
