package run.trama.saga.workflow

import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.PayloadValue
import run.trama.saga.RecordingHttp
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.RetryState
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.saga.httpCall
import run.trama.saga.v2Execution
import run.trama.telemetry.Metrics

class TaskNodeHandlerTest {

    private val def = SagaDefinitionV2("t", "1", FailureHandling.Retry(1, 1), "a", nodes = emptyList())
    private val payload = mapOf("orderId" to PayloadValue(JsonPrimitive("o-7")))

    private fun handler(http: RecordingHttp, async: Boolean = false) = TaskNodeHandler(
        renderer = MustacheTemplateRenderer(),
        httpClient = http.provider,
        metrics = Metrics(SimpleMeterRegistry()),
        callbackTokenService = if (async) CallbackTokenService("secret", "kid") else null,
        callbackUrlFactory = if (async) CallbackUrlFactory("http://trama.local/") else null,
    )

    private fun task(call: HttpCall, mode: TaskMode = TaskMode.SYNC, accepted: Set<Int>? = null, callback: CallbackConfig? = null) =
        TaskNode(id = "a", action = TaskAction(mode, call, accepted, callback))

    @Test
    fun `url, headers and body are rendered from payload`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("""{"ok":true}""", HttpStatusCode.OK) }
        val call = httpCall(
            url = "http://svc/orders/{{payload.orderId}}",
            body = """{"id":"{{payload.orderId}}"}""",
            headers = mapOf("X-Order" to "{{payload.orderId}}"),
        )
        val result = handler(http).execute(task(call), v2Execution(def, payload), payload, emptyList())

        assertIs<NodeResult.Advanced>(result.nodeResult)
        val req = http.requests.single()
        assertEquals(HttpMethod.Post, req.method)
        assertEquals("http://svc/orders/o-7", req.url.toString())
        assertEquals("o-7", req.headers["X-Order"])
        assertEquals("""{"id":"o-7"}""", String(req.body.toByteArray()))
        assertEquals("http://svc/orders/o-7", result.requestUrl)
        assertEquals("""{"id":"o-7"}""", result.requestBody)
        assertEquals("""{"ok":true}""", result.responseBody)
    }

    @Test
    fun `status outside successStatusCodes is a failure carrying status and body`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("""{"err":"nope"}""", HttpStatusCode.Conflict) }
        val result = handler(http).execute(task(httpCall("http://svc/x")), v2Execution(def), emptyMap(), emptyList())

        val failed = assertIs<NodeResult.NodeFailed>(result.nodeResult)
        assertEquals(409, failed.statusCode)
        assertEquals("""{"err":"nope"}""", failed.responseBody)
        assertTrue(failed.reason.message.contains("status=409"))
    }

    @Test
    fun `custom successStatusCodes are honored`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.NotFound) }
        val call = httpCall("http://svc/x", verb = HttpVerb.DELETE).copy(successStatusCodes = setOf(404))
        val result = handler(http).execute(task(call), v2Execution(def), emptyMap(), emptyList())
        assertIs<NodeResult.Advanced>(result.nodeResult)
    }

    @Test
    fun `default success codes depend on verb`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.Created) }
        val get = handler(http).execute(task(httpCall("http://svc/x", verb = HttpVerb.GET)), v2Execution(def), emptyMap(), emptyList())
        val post = handler(http).execute(task(httpCall("http://svc/x", verb = HttpVerb.POST)), v2Execution(def), emptyMap(), emptyList())
        assertIs<NodeResult.NodeFailed>(get.nodeResult, "GET only accepts 200 by default")
        assertIs<NodeResult.Advanced>(post.nodeResult, "POST accepts 201 by default")
    }

    @Test
    fun `transport exception becomes a retryable failure with error message`() = runBlocking<Unit> {
        val http = RecordingHttp { throw java.net.ConnectException("connection refused") }
        val result = handler(http).execute(task(httpCall("http://svc/x")), v2Execution(def), emptyMap(), emptyList())

        val failed = assertIs<NodeResult.NodeFailed>(result.nodeResult)
        assertTrue(failed.retryable)
        assertNull(result.statusCode)
        assertEquals("connection refused", result.error)
        assertEquals("http://svc/x", result.requestUrl)
    }

    @Test
    fun `compensate without compensation call is a no-op success`() = runBlocking<Unit> {
        val http = RecordingHttp { error("must not be called") }
        val result = handler(http).compensate(task(httpCall("http://svc/x")), v2Execution(def), emptyMap(), emptyList())
        assertIs<NodeResult.Advanced>(result.nodeResult)
        assertTrue(http.requests.isEmpty())
    }

    @Test
    fun `compensate calls compensation request`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.OK) }
        val node = task(httpCall("http://svc/x")).copy(compensation = httpCall("http://svc/undo/{{payload.orderId}}"))
        val result = handler(http).compensate(node, v2Execution(def, payload), payload, emptyList())
        assertIs<NodeResult.Advanced>(result.nodeResult)
        assertEquals("http://svc/undo/o-7", http.requests.single().url.toString())
    }

    @Test
    fun `async node injects callback url and token and waits`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.Accepted) }
        val call = httpCall(
            url = "http://svc/async",
            body = """{"cb":"{{runtime.callback.url}}","token":"{{runtime.callback.token}}"}""",
        )
        val exec = v2Execution(def)
        val result = handler(http, async = true).execute(
            task(call, TaskMode.ASYNC, callback = CallbackConfig(timeoutMillis = 60_000)),
            exec, emptyMap(), emptyList(),
        )

        val waiting = assertIs<NodeResult.WaitingForCallback>(result.nodeResult)
        assertEquals("a", waiting.nodeId)
        assertEquals(0, waiting.attempt)
        val body = String(http.requests.single().body.toByteArray())
        assertTrue(body.contains("http://trama.local/workflows/${exec.id}/node/a/callback"), body)
        assertTrue(body.contains(waiting.nonce), "token must embed the nonce: $body")
    }

    @Test
    fun `async attempt number comes from retry state`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.Accepted) }
        val exec = v2Execution(def, state = ExecutionState.InProgress(activeNodeId = "a", retry = RetryState.Applying(2, 0)))
        val result = handler(http, async = true).execute(
            task(httpCall("http://svc/async"), TaskMode.ASYNC, callback = CallbackConfig(60_000)),
            exec, emptyMap(), emptyList(),
        )
        assertEquals(2, assertIs<NodeResult.WaitingForCallback>(result.nodeResult).attempt)
    }

    @Test
    fun `async node with status outside acceptedStatusCodes fails without waiting`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.OK) }
        val result = handler(http, async = true).execute(
            task(httpCall("http://svc/async"), TaskMode.ASYNC, accepted = setOf(202), callback = CallbackConfig(60_000)),
            v2Execution(def), emptyMap(), emptyList(),
        )
        assertIs<NodeResult.NodeFailed>(result.nodeResult)
    }

    @Test
    fun `async node without callback service fails non-retryably`() = runBlocking<Unit> {
        val http = RecordingHttp { respond("", HttpStatusCode.Accepted) }
        val result = handler(http, async = false).execute(
            task(httpCall("http://svc/async"), TaskMode.ASYNC, callback = CallbackConfig(60_000)),
            v2Execution(def), emptyMap(), emptyList(),
        )
        val failed = assertIs<NodeResult.NodeFailed>(result.nodeResult)
        assertFalse(failed.retryable)
        assertTrue(http.requests.isEmpty())
        assertNotNull(failed.reason.message)
    }
}
