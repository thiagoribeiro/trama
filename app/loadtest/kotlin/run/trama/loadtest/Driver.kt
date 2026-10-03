package run.trama.loadtest

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import java.io.File
import java.util.Collections
import java.util.UUID
import kotlin.random.Random
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

/** Workflow shapes used by the scenarios. Every call carries X-Run so the mock can count effects. */
object Definitions {
    private fun call(url: String, body: JsonElement? = null) = buildJsonObject {
        put("url", url)
        put("verb", "POST")
        putJsonObject("headers") {
            put("Content-Type", "application/json")
            put("X-Run", "{{payload.runKey}}")
        }
        if (body != null) put("body", body)
    }

    /** Latency of compensation calls; scenario 2c raises it to kill workers mid-compensation. */
    var undoLatencyMs: Long = 0

    private fun task(id: String, url: String, next: String? = null, undo: Boolean = false) = buildJsonObject {
        put("kind", "task")
        put("id", id)
        putJsonObject("action") {
            put("mode", "sync")
            put("request", call(url))
        }
        if (undo) put("compensation", call("${Endpoints.MOCK}/undo/$id?latencyMs=$undoLatencyMs"))
        if (next != null) put("next", next)
    }

    private fun definition(name: String, entrypoint: String, nodes: List<JsonObject>) = buildJsonObject {
        put("name", name)
        put("version", "1")
        // No retries: every node should reach the downstream exactly once per phase, so any
        // extra call the mock records is an effect caused by a fault (redelivery), not by policy.
        putJsonObject("failureHandling") { put("type", "retry"); put("maxAttempts", 0); put("delayMillis", 0) }
        put("entrypoint", entrypoint)
        put("nodes", buildJsonArray { nodes.forEach { add(it) } })
    }

    /** (A) t1 → t2 → t3; t3 fails when payload.fail, compensating t1/t2. */
    fun chain(latencyMs: Long) = definition(
        "lt-chain", "t1",
        listOf(
            task("t1", "${Endpoints.MOCK}/sync/t1?latencyMs=$latencyMs", next = "t2", undo = true),
            task("t2", "${Endpoints.MOCK}/sync/t2?latencyMs=$latencyMs", next = "t3", undo = true),
            task("t3", "${Endpoints.MOCK}/sync/t3?latencyMs=$latencyMs&fail={{payload.fail}}"),
        ),
    )

    /**
     * (B/C) t0 → split[b-sync | b-async (callback) | b-sleep → b-sleep-task] → join → final.
     * final fails when payload.fail, compensating t0.
     */
    fun mixed(latencyMs: Long, callbackDelayMs: Long, sleepMs: Long) = definition(
        "lt-mixed", "t0",
        listOf(
            task("t0", "${Endpoints.MOCK}/sync/t0?latencyMs=$latencyMs", next = "fan", undo = true),
            buildJsonObject {
                put("kind", "split"); put("id", "fan")
                putJsonArray("branches") { add(JsonPrimitive("b-sync")); add(JsonPrimitive("b-async")); add(JsonPrimitive("b-sleep")) }
                put("join", "fan-in")
            },
            task("b-sync", "${Endpoints.MOCK}/sync/b-sync?latencyMs=$latencyMs"),
            buildJsonObject {
                put("kind", "task"); put("id", "b-async")
                putJsonObject("action") {
                    put("mode", "async")
                    put("request", call(
                        "${Endpoints.MOCK}/async/b-async?latencyMs=$latencyMs&callbackDelayMs=$callbackDelayMs",
                        buildJsonObject { put("callbackUrl", "{{runtime.callback.url}}"); put("callbackToken", "{{runtime.callback.token}}") },
                    ))
                    putJsonArray("acceptedStatusCodes") { add(JsonPrimitive(202)) }
                    putJsonObject("callback") { put("timeoutMillis", 120_000) }
                }
            },
            buildJsonObject { put("kind", "sleep"); put("id", "b-sleep"); put("durationMillis", sleepMs); put("next", "b-sleep-task") },
            task("b-sleep-task", "${Endpoints.MOCK}/sync/b-sleep-task?latencyMs=$latencyMs"),
            buildJsonObject { put("kind", "join"); put("id", "fan-in"); put("next", "final") },
            task("final", "${Endpoints.MOCK}/sync/final?latencyMs=$latencyMs&fail={{payload.fail}}"),
        ),
    )
}

data class RunRecord(val runKey: String, val sagaId: String?, val def: String, val fail: Boolean, val submitMs: Long, val http: Int, val error: String?)

fun readRuns(file: File): List<RunRecord> = file.readLines().drop(1).filter { it.isNotBlank() }.map { line ->
    val p = line.split(",", limit = 7)
    RunRecord(p[0], p[1].ifBlank { null }, p[2], p[3].toBoolean(), p[4].toLong(), p[5].toInt(), p.getOrNull(6)?.ifBlank { null })
}

/**
 * Submits --count workflows of --def (chain|mixed) at --rate per second (0 = as fast as
 * --concurrency allows), --failPct of them forced to fail. Appends to --out (runs.csv).
 */
fun runDriver(opts: Opts) = runBlocking {
    val api = opts.str("api", Endpoints.API)
    val count = opts.int("count", 100)
    val rate = opts.double("rate", 0.0)
    val concurrency = opts.int("concurrency", 64)
    val failPct = opts.double("failPct", 0.0)
    val defName = opts.str("def", "chain")
    val latency = opts.long("latencyMs", 20)
    Definitions.undoLatencyMs = opts.long("undoLatencyMs", 0)
    val definition = when (defName) {
        "chain" -> Definitions.chain(latency)
        "mixed" -> Definitions.mixed(latency, opts.long("callbackDelayMs", 2_000), opts.long("sleepMs", 3_000))
        else -> error("unknown def $defName")
    }
    val out = File(opts.str("out", "loadtest/run/runs.csv"))
    out.parentFile.mkdirs()
    if (!out.exists()) out.writeText("runKey,sagaId,def,fail,submitMs,http,error\n")

    // Pooled keep-alive connections: CIO opens one per request, which would charge Trama's API a
    // TCP accept per submitted run and skew throughput measurements.
    val client = HttpClient(OkHttp) {
        install(HttpTimeout) { requestTimeoutMillis = 30_000 }
        engine {
            config {
                dispatcher(okhttp3.Dispatcher().apply { maxRequests = concurrency * 2; maxRequestsPerHost = concurrency * 2 })
                connectionPool(okhttp3.ConnectionPool(concurrency * 2, 1, java.util.concurrent.TimeUnit.MINUTES))
            }
        }
    }
    val records = Collections.synchronizedList(mutableListOf<RunRecord>())
    val gate = Semaphore(concurrency)
    val start = System.currentTimeMillis()
    val progress = launch {
        while (true) {
            delay(5_000)
            log("submitted ${records.size}/$count (errors ${records.count { it.sagaId == null }})")
        }
    }
    (0 until count).map { i ->
        if (rate > 0) {
            val due = start + (i * 1000.0 / rate).toLong()
            val wait = due - System.currentTimeMillis()
            if (wait > 0) delay(wait)
        }
        gate.acquire()
        async {
            try {
                val runKey = UUID.randomUUID().toString()
                val fail = Random.nextDouble(100.0) < failPct
                val body = buildJsonObject {
                    put("definition", definition)
                    putJsonObject("payload") { put("runKey", runKey); put("fail", fail) }
                }
                val submitted = System.currentTimeMillis()
                val record = runCatching {
                    val resp = client.post("$api/workflows/run") { contentType(ContentType.Application.Json); setBody(body.toString()) }
                    val text = resp.bodyAsText()
                    val id = if (resp.status.value == 200) json.parseToJsonElement(text).jsonObject["id"]?.jsonPrimitive?.content else null
                    RunRecord(runKey, id, defName, fail, submitted, resp.status.value, if (id == null) text.take(120).replace(',', ';').replace('\n', ' ') else null)
                }.getOrElse { RunRecord(runKey, null, defName, fail, submitted, -1, (it.message ?: it.javaClass.simpleName).take(120).replace(',', ';').replace('\n', ' ')) }
                records += record
            } finally {
                gate.release()
            }
        }
    }.awaitAll()
    progress.cancel()
    val elapsed = (System.currentTimeMillis() - start) / 1000.0
    out.appendText(records.joinToString("") { "${it.runKey},${it.sagaId ?: ""},${it.def},${it.fail},${it.submitMs},${it.http},${it.error ?: ""}\n" })
    log("done: ${records.size} submitted in ${"%.1f".format(elapsed)}s (${"%.1f".format(records.size / elapsed)}/s), errors ${records.count { it.sagaId == null }}")
    client.close()
}
