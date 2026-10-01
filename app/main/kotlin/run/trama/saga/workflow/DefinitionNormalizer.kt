package run.trama.saga.workflow

import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode
import java.util.concurrent.ConcurrentHashMap

object DefinitionNormalizer {
    /**
     * Avoids re-normalizing the same definition on every node dispatch. Keyed by "name:version"
     * (bounded by the number of distinct definitions, as before), but an entry is only reused when
     * its source definition is structurally equal to the one being normalized: inline runs can
     * send different graphs under the same name/version, and those must never share an IR.
     * The equality check allocates nothing, unlike re-normalizing.
     */
    private class SourceCache<D : Any> {
        private val entries = ConcurrentHashMap<String, Pair<D, WorkflowDefinition>>()

        inline fun getOrNormalize(key: String, source: D, normalize: () -> WorkflowDefinition): WorkflowDefinition {
            entries[key]?.let { (cachedSource, workflow) -> if (cachedSource == source) return workflow }
            return normalize().also { entries[key] = source to it }
        }
    }

    private val v1Cache = SourceCache<SagaDefinition>()
    private val v2Cache = SourceCache<SagaDefinitionV2>()

    /**
     * Converts a v1 (steps-based) [SagaDefinition] into the common [WorkflowDefinition] IR.
     *
     * Each step becomes a [TaskNode] where:
     * - step.up  → action (SYNC mode)
     * - step.down → compensation
     * - next is chained by order; the last step has next = null (terminal)
     */
    fun normalize(definition: SagaDefinition): WorkflowDefinition =
        v1Cache.getOrNormalize("${definition.name}:${definition.version}", definition) { normalizeV1(definition) }

    private fun normalizeV1(definition: SagaDefinition): WorkflowDefinition {
        require(definition.steps.isNotEmpty()) {
            "SagaDefinition '${definition.name}' must have at least one step"
        }
        val steps = definition.steps
        val nodes: Map<String, WorkflowNode> = steps.mapIndexed { index, step ->
            val next = steps.getOrNull(index + 1)?.name
            val node = TaskNode(
                id = step.name,
                action = TaskAction(
                    mode = TaskMode.SYNC,
                    request = step.up,
                ),
                compensation = step.down,
                next = next,
            )
            step.name to node
        }.toMap()

        val workflow = WorkflowDefinition(
            name = definition.name,
            version = definition.version,
            failureHandling = definition.failureHandling,
            entrypoint = steps.first().name,
            nodes = nodes,
            onSuccessCallback = definition.onSuccessCallback,
            onFailureCallback = definition.onFailureCallback,
        )
        return workflow
    }

    /**
     * Converts a v2 (node-graph) [SagaDefinitionV2] into the common [WorkflowDefinition] IR.
     */
    fun normalize(definition: SagaDefinitionV2): WorkflowDefinition =
        v2Cache.getOrNormalize("${definition.name}:${definition.version}", definition) { normalizeV2(definition) }

    private fun normalizeV2(definition: SagaDefinitionV2): WorkflowDefinition {

        val nodes: Map<String, WorkflowNode> = definition.nodes.associate { nodeDef ->
            nodeDef.id to when (nodeDef) {
                is NodeDefinition.Task -> {
                    val action = nodeDef.action
                    val request = if (action.successStatusCodes != null) {
                        action.request.copy(successStatusCodes = action.successStatusCodes)
                    } else {
                        action.request
                    }
                    TaskNode(
                        id = nodeDef.id,
                        action = TaskAction(
                            mode = action.mode,
                            request = request,
                            acceptedStatusCodes = action.acceptedStatusCodes,
                            callbackConfig = action.callback?.let {
                                CallbackConfig(
                                    timeoutMillis = it.timeoutMillis,
                                    successWhen = it.successWhen,
                                    failureWhen = it.failureWhen,
                                )
                            },
                        ),
                        compensation = nodeDef.compensation,
                        next = nodeDef.next,
                    )
                }
                is NodeDefinition.Switch -> SwitchNode(
                    id = nodeDef.id,
                    cases = nodeDef.cases.map { c ->
                        SwitchCase(
                            name = c.name,
                            whenExpression = c.whenExpression,
                            target = c.target,
                        )
                    },
                    defaultTarget = nodeDef.default,
                )
                is NodeDefinition.Sleep -> SleepNode(
                    id = nodeDef.id,
                    durationMillis = nodeDef.durationMillis,
                    next = nodeDef.next,
                )
                is NodeDefinition.Split -> SplitNode(
                    id = nodeDef.id,
                    branches = nodeDef.branches,
                    join = nodeDef.join,
                )
                is NodeDefinition.Join -> JoinNode(
                    id = nodeDef.id,
                    next = nodeDef.next,
                )
            }
        }

        val workflow = WorkflowDefinition(
            name = definition.name,
            version = definition.version,
            failureHandling = definition.failureHandling,
            entrypoint = definition.entrypoint,
            nodes = nodes,
            onSuccessCallback = definition.onSuccessCallback,
            onFailureCallback = definition.onFailureCallback,
        )
        return workflow
    }
}
