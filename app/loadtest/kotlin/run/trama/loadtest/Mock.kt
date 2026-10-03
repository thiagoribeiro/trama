package run.trama.loadtest

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/**
 * Downstream service the load definitions call. Every request carries the run key
 * (X-Run: {{payload.runKey}}), so calls are counted per (run, node, phase): a count above one is
 * an external effect that happened more than once.
 *
 *   POST /sync/{node}?latencyMs=&fail=true|false   200 after latency (500 when fail=true)
 *   POST /async/{node}?latencyMs=&callbackDelayMs=  202, then POSTs body.callbackUrl with body.callbackToken
 *   POST /undo/{node}?latencyMs=                    compensation, 200 after latency
 *   GET  /calls      {"run|node|phase": count, ...}
 *   GET  /stats      totals, duplicates, in-flight calls, callback outcomes
 *   POST /reset
 */
fun runMock(opts: Opts) {
    val port = opts.int("port", 7070)
    val calls = ConcurrentHashMap<String, AtomicInteger>()
    val inflight = ConcurrentHashMap<String, AtomicInteger>()
    val callbackOutcomes = ConcurrentHashMap<Int, AtomicLong>()
    val total = AtomicLong()
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val client = HttpClient(CIO) { engine { maxConnectionsCount = 2_000 } }

    fun record(run: String, node: String, phase: String): String {
        val key = "$run|$node|$phase"
        calls.computeIfAbsent(key) { AtomicInteger() }.incrementAndGet()
        total.incrementAndGet()
        return key
    }

    suspend fun <T> tracked(key: String, block: suspend () -> T): T {
        inflight.computeIfAbsent(key) { AtomicInteger() }.incrementAndGet()
        try { return block() } finally { inflight[key]?.decrementAndGet() }
    }

    embeddedServer(Netty, port = port, host = "127.0.0.1") {
        routing {
            post("/sync/{node}") {
                val node = call.parameters["node"]!!
                val run = call.request.headers["X-Run"] ?: "unknown"
                val latency = call.request.queryParameters["latencyMs"]?.toLongOrNull() ?: 0
                val fail = call.request.queryParameters["fail"] == "true"
                val key = record(run, node, "UP")
                tracked(key) { if (latency > 0) delay(latency) }
                if (fail) call.respondText("""{"error":"forced"}""", ContentType.Application.Json, HttpStatusCode.InternalServerError)
                else call.respondText("""{"ok":true,"node":"$node"}""", ContentType.Application.Json)
            }
            post("/undo/{node}") {
                val key = record(call.request.headers["X-Run"] ?: "unknown", call.parameters["node"]!!, "DOWN")
                val latency = call.request.queryParameters["latencyMs"]?.toLongOrNull() ?: 0
                tracked(key) { if (latency > 0) delay(latency) }
                call.respondText("""{"ok":true}""", ContentType.Application.Json)
            }
            post("/async/{node}") {
                val node = call.parameters["node"]!!
                val run = call.request.headers["X-Run"] ?: "unknown"
                val latency = call.request.queryParameters["latencyMs"]?.toLongOrNull() ?: 0
                val callbackDelay = call.request.queryParameters["callbackDelayMs"]?.toLongOrNull() ?: 1_000
                val body = json.parseToJsonElement(call.receiveText()).jsonObject
                val url = body["callbackUrl"]!!.jsonPrimitive.content
                val token = body["callbackToken"]!!.jsonPrimitive.content
                val key = record(run, node, "UP")
                tracked(key) { if (latency > 0) delay(latency) }
                call.respondText("""{"accepted":true}""", ContentType.Application.Json, HttpStatusCode.Accepted)
                scope.launch {
                    delay(callbackDelay)
                    val status = runCatching {
                        client.post(url) {
                            header("X-Callback-Token", token)
                            header("Content-Type", "application/json")
                            setBody("""{"status":"ok"}""")
                        }.status.value
                    }.getOrDefault(-1)
                    record(run, node, "CALLBACK_SENT")
                    callbackOutcomes.computeIfAbsent(status) { AtomicLong() }.incrementAndGet()
                }
            }
            get("/calls") {
                val body = JsonObject(calls.mapValues { JsonPrimitive(it.value.get()) })
                call.respondText(body.toString(), ContentType.Application.Json)
            }
            get("/stats") {
                val dups = calls.filter { it.value.get() > 1 && !it.key.endsWith("|CALLBACK_SENT") }
                val stats = buildJsonObject {
                    put("totalCalls", total.get())
                    put("distinctKeys", calls.size)
                    put("duplicateKeys", dups.size)
                    put("extraCalls", dups.values.sumOf { it.get() - 1 })
                    put("inflight", JsonObject(inflight.filter { it.value.get() > 0 }.mapValues { JsonPrimitive(it.value.get()) }))
                    put("callbackOutcomes", JsonObject(callbackOutcomes.mapKeys { it.key.toString() }.mapValues { JsonPrimitive(it.value.get()) }))
                }
                call.respondText(stats.toString(), ContentType.Application.Json)
            }
            post("/reset") {
                calls.clear(); inflight.clear(); callbackOutcomes.clear(); total.set(0)
                call.respondText("{}", ContentType.Application.Json)
            }
        }
    }.start(wait = false)
    log("mock listening on 127.0.0.1:$port")
    Thread.currentThread().join()
}
