package run.trama.saga.workflow

import kotlinx.serialization.json.JsonElement
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.TaskMode

/**
 * Internal Representation (IR) of a workflow definition.
 * All execution logic operates on this model regardless of whether
 * the incoming definition was v1 (steps) or v2 (nodes).
 */
data class WorkflowDefinition(
    val name: String,
    val version: String,
    val failureHandling: FailureHandling,
    val entrypoint: String,
    val nodes: Map<String, WorkflowNode>,
    val onSuccessCallback: HttpCall? = null,
    val onFailureCallback: HttpCall? = null,
)

sealed class WorkflowNode {
    abstract val id: String
}

data class TaskNode(
    override val id: String,
    val action: TaskAction,
    val compensation: HttpCall? = null,
    /** null means this is the terminal node */
    val next: String? = null,
) : WorkflowNode()

data class SwitchNode(
    override val id: String,
    val cases: List<SwitchCase>,
    val defaultTarget: String,
) : WorkflowNode()

data class SwitchCase(
    val name: String?,
    /** Raw json-logic expression evaluated as boolean */
    val whenExpression: JsonElement,
    val target: String,
)

data class TaskAction(
    val mode: TaskMode,
    val request: HttpCall,
    /** For ASYNC mode: HTTP status codes that mean "request accepted, waiting for callback" */
    val acceptedStatusCodes: Set<Int>? = null,
    val callbackConfig: CallbackConfig? = null,
)

data class CallbackConfig(
    val timeoutMillis: Long,
    /** json-logic expression: if true → node succeeded */
    val successWhen: JsonElement? = null,
    /** json-logic expression: if true → node failed */
    val failureWhen: JsonElement? = null,
)
