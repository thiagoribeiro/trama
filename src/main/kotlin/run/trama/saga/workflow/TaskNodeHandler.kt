package run.trama.saga.workflow

import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpMethod
import io.opentelemetry.api.trace.SpanKind
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.FailureReason
import run.trama.saga.HttpCall
import run.trama.saga.HttpClientProvider
import run.trama.saga.HttpVerb
import run.trama.saga.PayloadValue
import run.trama.saga.RetryState
import run.trama.saga.SagaExecution
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.TemplateContextBuilder
import run.trama.saga.TemplateRenderer
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.telemetry.Metrics
import run.trama.telemetry.Tracing
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv
import java.time.Instant

class TaskNodeHandler(
    private val renderer: TemplateRenderer,
    private val httpClient: HttpClientProvider,
    private val metrics: Metrics,
    /** Required for ASYNC mode nodes; null disables async support. */
    private val callbackTokenService: CallbackTokenService? = null,
    private val callbackUrlFactory: CallbackUrlFactory? = null,
) {
    private val logger = LoggerFactory.getLogger(TaskNodeHandler::class.java)
    private val tracer = Tracing.tracer("task-node-handler")

    /**
     * Executes the forward action of a [TaskNode] (SYNC mode only in PR2).
     * Returns [NodeResult.Advanced] on success or [NodeResult.NodeFailed] on failure.
     *
     * Also returns raw response data ([statusCode], [responseBody]) so the caller
     * can persist the step result.
     */
    suspend fun execute(
        node: TaskNode,
        execution: SagaExecution,
        payload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
    ): TaskHttpResult {
        return if (node.action.mode == TaskMode.ASYNC) {
            executeAsync(node, execution, payload, stepResults)
        } else {
            val context = TemplateContextBuilder.build(execution, node.id, ExecutionPhase.UP, stepResults, payload)
            executeCall(node.id, ExecutionPhase.UP, node.action.request, context, execution)
        }
    }

    private suspend fun executeAsync(
        node: TaskNode,
        execution: SagaExecution,
        payload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
    ): TaskHttpResult {
        val callbackConfig = node.action.callbackConfig
        val tokenSvc = callbackTokenService
        if (callbackConfig == null || tokenSvc == null) {
            return TaskHttpResult(
                statusCode = null,
                responseBody = null,
                nodeResult = NodeResult.NodeFailed(
                    retryable = false,
                    reason = FailureReason("async node '${node.id}' requires callbackConfig and callback service configuration"),
                ),
            )
        }

        val attempt = (execution.state as? ExecutionState.InProgress)
            ?.retry?.let { (it as? RetryState.Applying)?.attempt } ?: 0

        val meta = tokenSvc.generate(
            executionId = execution.id,
            nodeId = node.id,
            attempt = attempt,
            timeoutMillis = callbackConfig.timeoutMillis,
        )
        val callbackUrl = callbackUrlFactory?.buildUrl(execution.id, node.id) ?: ""
        val token = tokenSvc.tokenString(meta)

        val baseContext = TemplateContextBuilder.build(execution, node.id, ExecutionPhase.UP, stepResults, payload)
        val context = baseContext + mapOf(
            "runtime" to mapOf(
                "callback" to mapOf("url" to callbackUrl, "token" to token)
            )
        )

        val acceptedCodes = node.action.acceptedStatusCodes ?: setOf(202)
        val call = node.action.request.copy(successStatusCodes = acceptedCodes)
        val httpResult = executeCall(node.id, ExecutionPhase.UP, call, context, execution)

        if (httpResult.nodeResult is NodeResult.NodeFailed) {
            return httpResult // request failed — surface error, don't wait for callback
        }

        return TaskHttpResult(
            statusCode = httpResult.statusCode,
            responseBody = httpResult.responseBody,
            nodeResult = NodeResult.WaitingForCallback(
                nodeId = node.id,
                attempt = attempt,
                deadlineAt = meta.expiresAt,
                nonce = meta.nonce,
                signature = meta.signature,
            ),
        )
    }

    /**
     * Executes the compensation action of a [TaskNode].
     */
    suspend fun compensate(
        node: TaskNode,
        execution: SagaExecution,
        payload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
    ): TaskHttpResult {
        val call = node.compensation ?: return TaskHttpResult(
            statusCode = null,
            responseBody = null,
            nodeResult = NodeResult.Advanced(null),
        )
        val context = TemplateContextBuilder.build(execution, node.id, ExecutionPhase.DOWN, stepResults, payload)
        return executeCall(node.id, ExecutionPhase.DOWN, call, context, execution)
    }

    private suspend fun executeCall(
        nodeName: String,
        phase: ExecutionPhase,
        call: HttpCall,
        context: Map<String, Any?>,
        execution: SagaExecution,
    ): TaskHttpResult {
        val url = renderer.render(call.url, context)
        return Tracing.withSpan(
            tracer = tracer,
            name = "node.execute",
            kind = SpanKind.CLIENT,
            attributes = mapOf(
                "saga.id" to execution.id.toString(),
                "node.id" to nodeName,
                "node.phase" to phase.name,
                "http.method" to call.verb.name,
                "http.url" to url,
            ),
        ) { span ->
            Tracing.withTraceMdc(span, execution.id.toString()) {
                logger.info(
                    "executing node",
                    kv("nodeId", nodeName),
                    kv("phase", phase.name),
                    kv("url", url),
                    kv("method", call.verb.name),
                )
            }
            try {
                val startNanos = System.nanoTime()
                val response = httpClient.client.request(url) {
                    method = call.verb.toKtorMethod()
                    Tracing.injectHeaders { k, v -> header(k, v) }
                    call.headers.forEach { (k, v) -> header(k, renderer.render(v, context)) }
                    call.body?.let { setBody(renderer.render(it, context)) }
                }
                val body = response.bodyAsText()
                val success = response.status.value in call.successStatusCodes
                if (success && phase == ExecutionPhase.UP) {
                    metrics.recordStepSuccessDuration(
                        sagaName = execution.definition.name,
                        sagaVersion = execution.definition.version,
                        stepName = nodeName,
                        durationNanos = System.nanoTime() - startNanos,
                    )
                }
                Tracing.withTraceMdc(span, execution.id.toString()) {
                    logger.info(
                        "node completed",
                        kv("nodeId", nodeName),
                        kv("phase", phase.name),
                        kv("status", response.status.value),
                    )
                }
                val nodeResult = if (success) {
                    NodeResult.Advanced(null)  // caller sets next
                } else {
                    NodeResult.NodeFailed(
                        retryable = true,
                        reason = FailureReason("node=$nodeName status=${response.status.value}"),
                        statusCode = response.status.value,
                        responseBody = body,
                    )
                }
                TaskHttpResult(
                    statusCode = response.status.value,
                    responseBody = body,
                    nodeResult = nodeResult,
                )
            } catch (ex: Exception) {
                span.recordException(ex)
                Tracing.withTraceMdc(span, execution.id.toString()) {
                    logger.warn(
                        "node failed",
                        kv("nodeId", nodeName),
                        kv("phase", phase.name),
                        kv("error", ex.message ?: "unknown"),
                    )
                }
                TaskHttpResult(
                    statusCode = null,
                    responseBody = null,
                    nodeResult = NodeResult.NodeFailed(
                        retryable = true,
                        reason = FailureReason("node=$nodeName error=${ex.message ?: "unknown"}"),
                    ),
                )
            }
        }
    }

}

/** Maps [HttpVerb] to the Ktor [HttpMethod] counterpart. Shared across all node handlers. */
fun HttpVerb.toKtorMethod(): HttpMethod = when (this) {
    HttpVerb.GET -> HttpMethod.Get
    HttpVerb.POST -> HttpMethod.Post
    HttpVerb.PUT -> HttpMethod.Put
    HttpVerb.PATCH -> HttpMethod.Patch
    HttpVerb.DELETE -> HttpMethod.Delete
    HttpVerb.HEAD -> HttpMethod.Head
    HttpVerb.OPTIONS -> HttpMethod.Options
}

data class TaskHttpResult(
    val statusCode: Int?,
    val responseBody: String?,
    val nodeResult: NodeResult,
)
