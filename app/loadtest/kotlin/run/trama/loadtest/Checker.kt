package run.trama.loadtest

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject

private val TERMINAL = setOf("SUCCEEDED", "FAILED", "CORRUPTED")

/** External effects each definition must produce exactly once (key suffix "node|phase"). */
private fun expectedEffects(def: String, fail: Boolean): Set<String> = when (def) {
    "chain" -> if (fail) setOf("t1|UP", "t2|UP", "t3|UP", "t1|DOWN", "t2|DOWN") else setOf("t1|UP", "t2|UP", "t3|UP")
    "mixed" -> setOf("t0|UP", "b-sync|UP", "b-async|UP", "b-async|CALLBACK_SENT", "b-sleep-task|UP", "final|UP") +
        (if (fail) setOf("t0|DOWN") else emptySet())
    else -> emptySet()
}

private data class Row(val status: String, val startedMs: Long, val completedMs: Long?)

private fun Connection.statuses(ids: List<UUID>): Map<UUID, Row> {
    if (ids.isEmpty()) return emptyMap()
    val out = HashMap<UUID, Row>()
    // Latest row per id (an id is unique in practice; ORDER keeps it deterministic).
    prepareStatement(
        "SELECT DISTINCT ON (id) id, status, started_at, completed_at FROM saga_execution WHERE id = ANY(?) ORDER BY id, started_at DESC"
    ).use { ps ->
        ps.setArray(1, createArrayOf("uuid", ids.toTypedArray()))
        ps.executeQuery().use { rs ->
            while (rs.next()) {
                out[rs.getObject(1, UUID::class.java)] = Row(
                    rs.getString(2),
                    rs.getTimestamp(3).time,
                    rs.getTimestamp(4)?.time,
                )
            }
        }
    }
    return out
}

private fun Connection.longQuery(sql: String, ids: List<UUID>): List<List<String>> =
    prepareStatement(sql).use { ps ->
        ps.setArray(1, createArrayOf("uuid", ids.toTypedArray()))
        ps.executeQuery().use { rs ->
            val cols = rs.metaData.columnCount
            buildList { while (rs.next()) add((1..cols).map { rs.getString(it) ?: "" }) }
        }
    }

private fun percentile(sorted: List<Long>, p: Double): Long =
    if (sorted.isEmpty()) 0 else sorted[((p / 100.0) * (sorted.size - 1)).toInt()]

/**
 * Waits up to --waitSec for every submitted run (--runs) to reach a terminal status, then
 * reports outcomes, stuck/lost executions, duplicate or missing external effects (mock), duplicate
 * step rows, and end-to-end latency. Writes a JSON summary to --out.
 */
fun runChecker(opts: Opts) = runBlocking {
    val runs = readRuns(File(opts.str("runs", "loadtest/run/runs.csv")))
    val submitted = runs.filter { it.sagaId != null }
    val ids = submitted.map { UUID.fromString(it.sagaId) }
    val waitSec = opts.long("waitSec", 120)
    val conn = DriverManager.getConnection(opts.str("db", Endpoints.DB), "saga", "saga")

    val deadline = System.currentTimeMillis() + waitSec * 1000
    var rows = conn.statuses(ids)
    while (System.currentTimeMillis() < deadline) {
        val pending = ids.count { rows[it]?.status !in TERMINAL }
        if (pending == 0) break
        log("waiting: $pending of ${ids.size} not terminal yet")
        Thread.sleep(3_000)
        rows = conn.statuses(ids)
    }

    val children = conn.longQuery("SELECT parent_id, child_id FROM saga_join_branch WHERE parent_id = ANY(?)", ids)
        .map { UUID.fromString(it[1]) }
    val childRows = conn.statuses(children)
    val allIds = ids + children
    val dupSteps = conn.longQuery(
        "SELECT saga_id, step_name, phase, count(*) FROM saga_step_result WHERE saga_id = ANY(?) GROUP BY 1,2,3 HAVING count(*) > 1",
        allIds,
    )

    val calls = HttpClient(CIO).use { client ->
        json.parseToJsonElement(client.get("${opts.str("mock", Endpoints.MOCK)}/calls").bodyAsText()).jsonObject
            .mapValues { it.value.jsonPrimitive.int }
    }
    val byRun = calls.entries.groupBy({ it.key.substringBefore('|') }, { it.key.substringAfter('|') to it.value })

    var extraCalls = 0
    var runsWithDuplicates = 0
    var missingEffects = 0
    val outcome = mutableMapOf<String, Int>()
    var wrongOutcome = 0
    val latencies = mutableListOf<Long>()
    for (run in submitted) {
        val row = rows[UUID.fromString(run.sagaId)]
        val status = row?.status ?: "MISSING"
        outcome.merge(status, 1, Int::plus)
        val expectedStatus = if (run.fail) "FAILED" else "SUCCEEDED"
        if (status in TERMINAL && status != expectedStatus) wrongOutcome++
        if (row?.completedMs != null && status in TERMINAL) latencies += row.completedMs - row.startedMs
        val effects = byRun[run.runKey].orEmpty().toMap()
        val extra = effects.values.sumOf { (it - 1).coerceAtLeast(0) }
        extraCalls += extra
        if (extra > 0) runsWithDuplicates++
        if (status == expectedStatus) missingEffects += expectedEffects(run.def, run.fail).count { (effects[it] ?: 0) == 0 }
    }
    latencies.sort()
    val terminalRows = submitted.mapNotNull { rows[UUID.fromString(it.sagaId)] }.filter { it.status in TERMINAL && it.completedMs != null }
    val windowSec = if (terminalRows.isEmpty()) 0.0 else
        (terminalRows.maxOf { it.completedMs!! } - terminalRows.minOf { it.startedMs }) / 1000.0

    val summary = buildJsonObject {
        put("runs", runs.size)
        put("submitFailed", runs.size - submitted.size)
        putJsonObject("status") { outcome.toSortedMap().forEach { (k, v) -> put(k, v) } }
        putJsonObject("statusByDef") {
            submitted.groupBy { it.def }.toSortedMap().forEach { (def, rs) ->
                putJsonObject(def) {
                    rs.groupingBy { rows[UUID.fromString(it.sagaId)]?.status ?: "MISSING" }.eachCount().toSortedMap().forEach { (k, v) -> put(k, v) }
                }
            }
        }
        put("stuck", submitted.count { rows[UUID.fromString(it.sagaId)]?.status.let { s -> s != null && s !in TERMINAL } })
        put("lost", submitted.count { rows[UUID.fromString(it.sagaId)] == null })
        put("wrongOutcome", wrongOutcome)
        put("childExecutions", children.size)
        put("childrenNotTerminal", children.count { childRows[it]?.status !in TERMINAL })
        put("extraExternalCalls", extraCalls)
        put("runsWithDuplicateCalls", runsWithDuplicates)
        put("missingEffectsOnCorrectOutcome", missingEffects)
        put("duplicateStepRows", dupSteps.size)
        put("throughputPerSec", if (windowSec > 0) "%.1f".format(terminalRows.size / windowSec).toDouble() else 0.0)
        putJsonObject("latencyMs") {
            put("p50", percentile(latencies, 50.0)); put("p95", percentile(latencies, 95.0))
            put("p99", percentile(latencies, 99.0)); put("max", latencies.lastOrNull() ?: 0)
        }
    }
    val pretty = kotlinx.serialization.json.Json { prettyPrint = true }.encodeToString(kotlinx.serialization.json.JsonObject.serializer(), summary)
    println(pretty)
    opts.str("out", "").takeIf { it.isNotBlank() }?.let { File(it).apply { parentFile?.mkdirs() }.writeText(pretty) }
    if (dupSteps.isNotEmpty()) log("sample duplicate step rows: ${dupSteps.take(5)}")
    conn.close()
}
