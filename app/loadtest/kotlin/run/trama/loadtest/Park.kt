package run.trama.loadtest

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import java.io.File
import java.util.UUID
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

/**
 * Scenario 1c: submits --perState executions designed to sit in each state for about --holdSec,
 * so a Redis fault can be injected while they are parked. Labels (the runs.csv `def` column):
 *   park-running   a sync HTTP call in flight (mock latency = min(hold, 20s), under the HTTP timeout)
 *   park-sleeping  t1 → sleep(hold) → t2
 *   park-callback  async node whose callback arrives after hold → t2
 *   park-join      split[ sync | sleep(hold) → task ] → join → t2   (parent WAITING_JOIN)
 *   park-backoff   t1 always fails, retried once after hold, then FAILED (expected)
 */
fun runPark(opts: Opts) = runBlocking {
    val api = opts.str("api", Endpoints.API)
    val per = opts.int("perState", 20)
    val holdMs = opts.long("holdSec", 60) * 1000
    val out = File(opts.str("out", "loadtest/run/runs.csv")).apply { parentFile.mkdirs() }
    if (!out.exists()) out.writeText("runKey,sagaId,def,fail,submitMs,http,error\n")
    val m = Endpoints.MOCK

    fun call(url: String, body: JsonObject? = null) = buildJsonObject {
        put("url", url); put("verb", "POST")
        putJsonObject("headers") { put("Content-Type", "application/json"); put("X-Run", "{{payload.runKey}}") }
        if (body != null) put("body", body)
    }
    fun task(id: String, url: String, next: String? = null) = buildJsonObject {
        put("kind", "task"); put("id", id)
        putJsonObject("action") { put("mode", "sync"); put("request", call(url)) }
        if (next != null) put("next", next)
    }
    fun def(label: String, entry: String, nodes: List<JsonObject>, retries: Int = 0, delayMs: Long = 0) = buildJsonObject {
        put("name", label); put("version", "1")
        putJsonObject("failureHandling") { put("type", "retry"); put("maxAttempts", retries); put("delayMillis", delayMs) }
        put("entrypoint", entry)
        put("nodes", buildJsonArray { nodes.forEach { add(it) } })
    }
    val defs = mapOf(
        // Below Trama's 30s HTTP request timeout, so the call itself never fails.
        "park-running" to (def("park-running", "t1", listOf(task("t1", "$m/sync/t1?latencyMs=${minOf(holdMs, 20_000)}"))) to false),
        "park-sleeping" to (def("park-sleeping", "t1", listOf(
            task("t1", "$m/sync/t1", next = "nap"),
            buildJsonObject { put("kind", "sleep"); put("id", "nap"); put("durationMillis", holdMs); put("next", "t2") },
            task("t2", "$m/sync/t2"),
        )) to false),
        "park-callback" to (def("park-callback", "cb", listOf(
            buildJsonObject {
                put("kind", "task"); put("id", "cb")
                putJsonObject("action") {
                    put("mode", "async")
                    put("request", call("$m/async/cb?callbackDelayMs=$holdMs",
                        buildJsonObject { put("callbackUrl", "{{runtime.callback.url}}"); put("callbackToken", "{{runtime.callback.token}}") }))
                    putJsonArray("acceptedStatusCodes") { add(JsonPrimitive(202)) }
                    putJsonObject("callback") { put("timeoutMillis", holdMs * 2) }
                }
                put("next", "t2")
            },
            task("t2", "$m/sync/t2"),
        )) to false),
        "park-join" to (def("park-join", "fan", listOf(
            buildJsonObject {
                put("kind", "split"); put("id", "fan")
                putJsonArray("branches") { add(JsonPrimitive("b1")); add(JsonPrimitive("b2")) }
                put("join", "fan-in")
            },
            task("b1", "$m/sync/b1"),
            buildJsonObject { put("kind", "sleep"); put("id", "b2"); put("durationMillis", holdMs); put("next", "b2-task") },
            task("b2-task", "$m/sync/b2-task"),
            buildJsonObject { put("kind", "join"); put("id", "fan-in"); put("next", "t2") },
            task("t2", "$m/sync/t2"),
        )) to false),
        "park-backoff" to (def("park-backoff", "t1", listOf(task("t1", "$m/sync/t1?fail=true")), retries = 1, delayMs = holdMs) to true),
    )

    HttpClient(CIO).use { client ->
        for ((label, pair) in defs) {
            val (definition, expectFail) = pair
            repeat(per) {
                val runKey = UUID.randomUUID().toString()
                val body = buildJsonObject {
                    put("definition", definition)
                    putJsonObject("payload") { put("runKey", runKey); put("fail", expectFail) }
                }
                val resp = client.post("$api/workflows/run") { contentType(ContentType.Application.Json); setBody(body.toString()) }
                val id = if (resp.status.value == 200) json.parseToJsonElement(resp.bodyAsText()).jsonObject["id"]?.jsonPrimitive?.content else null
                out.appendText("$runKey,${id ?: ""},$label,$expectFail,${System.currentTimeMillis()},${resp.status.value},\n")
            }
            log("parked $per x $label")
        }
    }
}
