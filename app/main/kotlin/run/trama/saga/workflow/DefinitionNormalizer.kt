package run.trama.saga.workflow

import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode
import java.util.concurrent.ConcurrentHashMap

object DefinitionNormalizer {
    // Keyed by "name:version" — avoids re-normalizing the same definition on every node dispatch.
    private val v1Cache = ConcurrentHashMap<String, WorkflowDefinition>()
    private val v2Cache = ConcurrentHashMap<String, WorkflowDefinition>()

    /**
     * Converts a v1 (steps-based) [SagaDefinition] into the common [WorkflowDefinition] IR.
     *
     * Each step becomes a [TaskNode] where:
     * - step.up  → action (SYNC mode)
     * - step.down → compensation
     * - next is chained by order; the last step has next = null (terminal)
     */
    fun normalize(definition: SagaDefinition): WorkflowDefinition {
        val cacheKey = "${definition.name}:${definition.version}"
        v1Cache[cacheKey]?.let { return it }

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
        v1Cache[cacheKey] = workflow
        return workflow
    }

    /**
     * Converts a v2 (node-graph) [SagaDefinitionV2] into the common [WorkflowDefinition] IR.
     */
    fun normalize(definition: SagaDefinitionV2): WorkflowDefinition {
        val cacheKey = "${definition.name}:${definition.version}"
        v2Cache[cacheKey]?.let { return it }

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
        v2Cache[cacheKey] = workflow
        return workflow
    }
}
