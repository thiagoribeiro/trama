package run.trama.loadtest

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.lettuce.core.RedisClient
import java.io.File
import java.sql.DriverManager
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/** Sums every sample of [metric] (all label sets) in a Prometheus text scrape. */
private fun promSum(scrape: String, metric: String): Double =
    scrape.lineSequence()
        .filter { it.startsWith(metric) && (it.getOrNull(metric.length) == '{' || it.getOrNull(metric.length) == ' ') }
        .sumOf { it.substringAfterLast(' ').toDoubleOrNull() ?: 0.0 }

private data class Proc(val idx: Int, val pid: Long, val port: Int)

private fun readProcs(file: File): List<Proc> =
    if (!file.exists()) emptyList() else file.readLines().filter { it.isNotBlank() }.map {
        val (i, pid, port) = it.trim().split(Regex("\\s+"))
        Proc(i.toInt(), pid.toLong(), port.toInt())
    }

/** utime+stime in clock ticks, and RSS in KB, from /proc. Null when the process is gone. */
private fun procStats(pid: Long): Pair<Long, Long>? = runCatching {
    val stat = File("/proc/$pid/stat").readText().substringAfterLast(')').trim().split(' ')
    val ticks = stat[11].toLong() + stat[12].toLong()
    val rss = File("/proc/$pid/status").readLines().first { it.startsWith("VmRSS:") }.split(Regex("\\s+"))[1].toLong()
    ticks to rss
}.getOrNull()

/**
 * Samples every --intervalSec until killed or --durationSec elapses, appending long-format rows
 * (epochMs,source,metric,value) to --out: per Trama process (queue counters, CPU %, RSS MB),
 * Redis (ops/s, memory, ready/in-flight queue depth) and Postgres (commits, inserts, connections).
 */
fun runCollector(opts: Opts) = runBlocking {
    val interval = opts.long("intervalSec", 5)
    val duration = opts.long("durationSec", 7 * 24 * 3600) // effectively "until killed"; must not overflow below
    val procsFile = File(opts.str("pids", "loadtest/run/pids"))
    val out = File(opts.str("out", "loadtest/run/metrics.csv")).apply { parentFile.mkdirs() }
    if (!out.exists()) out.writeText("epochMs,source,metric,value\n")
    val prefix = opts.str("queuePrefix", "saga:executions")
    val shards = opts.int("shards", 1024)
    val http = HttpClient(CIO) { install(HttpTimeout) { requestTimeoutMillis = 2_000 } }
    val redis = RedisClient.create(opts.str("redis", Endpoints.REDIS)).connect().async()
    val db = opts.str("db", Endpoints.DB)
    val hz = 100.0 // USER_HZ on Linux
    val lastTicks = HashMap<Long, Pair<Long, Long>>()
    val queueKeys = (0 until shards).map { "{vs-${it.toString().padStart(4, '0')}}" }
    val end = System.currentTimeMillis() + duration * 1000

    while (System.currentTimeMillis() < end) {
        val now = System.currentTimeMillis()
        val rows = StringBuilder()
        fun row(source: String, metric: String, value: Number) = rows.append("$now,$source,$metric,$value\n")

        for (p in readProcs(procsFile)) {
            val src = "trama-${p.idx}"
            procStats(p.pid)?.let { (ticks, rssKb) ->
                lastTicks[p.pid]?.let { (prevTicks, prevAt) ->
                    val secs = (now - prevAt) / 1000.0
                    if (secs > 0) row(src, "cpu_pct", Math.round((ticks - prevTicks) / hz / secs * 1000) / 10.0)
                }
                lastTicks[p.pid] = ticks to now
                row(src, "rss_mb", rssKb / 1024)
            } ?: row(src, "alive", 0)
            runCatching { http.get("http://127.0.0.1:${p.port}/metrics").bodyAsText() }.getOrNull()?.let { s ->
                row(src, "enqueued_total", promSum(s, "saga_enqueue_total"))
                row(src, "dequeued_total", promSum(s, "saga_dequeue_total"))
                row(src, "processed_total", promSum(s, "saga_processed_total"))
                row(src, "owned_shards", promSum(s, "saga_redis_owned_shards"))
            }
        }

        runCatching {
            val info = redis.info("all").get().lines().associate { it.substringBefore(':') to it.substringAfter(':').trim() }
            row("redis", "ops_per_sec", info["instantaneous_ops_per_sec"]?.toLongOrNull() ?: 0)
            row("redis", "commands_total", info["total_commands_processed"]?.toLongOrNull() ?: 0)
            row("redis", "used_memory_mb", (info["used_memory"]?.toLongOrNull() ?: 0) / (1024 * 1024))
            val ready = queueKeys.map { redis.zcard("$prefix:$it:ready") }
            val inflight = queueKeys.map { redis.zcard("$prefix:$it:inflight") }
            row("redis", "queue_ready", ready.sumOf { it.get() })
            row("redis", "queue_inflight", inflight.sumOf { it.get() })
        }.onFailure { row("redis", "unreachable", 1) }

        runCatching {
            DriverManager.getConnection(db, "saga", "saga").use { c ->
                c.createStatement().use { st ->
                    st.executeQuery("SELECT xact_commit, tup_inserted, tup_updated, numbackends FROM pg_stat_database WHERE datname = 'saga'").use { rs ->
                        if (rs.next()) {
                            row("postgres", "xact_commit_total", rs.getLong(1))
                            row("postgres", "tup_inserted_total", rs.getLong(2))
                            row("postgres", "tup_updated_total", rs.getLong(3))
                            row("postgres", "connections", rs.getLong(4))
                        }
                    }
                }
            }
        }.onFailure { row("postgres", "unreachable", 1) }

        out.appendText(rows.toString())
        delay(interval * 1000)
    }
    http.close()
}

/** Top statements by total execution time (requires pg_stat_statements, enabled by stack.sh). */
fun runPgTop(opts: Opts) {
    DriverManager.getConnection(opts.str("db", Endpoints.DB), "saga", "saga").use { c ->
        c.createStatement().use { st ->
            if (opts.str("reset", "false") == "true") { st.execute("SELECT pg_stat_statements_reset()"); log("pg_stat_statements reset"); return }
            st.executeQuery(
                """
                SELECT calls, round(total_exec_time::numeric, 1), round(mean_exec_time::numeric, 3), rows,
                       left(regexp_replace(query, '\s+', ' ', 'g'), 140)
                FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'saga')
                ORDER BY total_exec_time DESC LIMIT ${opts.int("limit", 15)}
                """.trimIndent()
            ).use { rs ->
                println("calls\ttotal_ms\tmean_ms\trows\tquery")
                while (rs.next()) println("${rs.getLong(1)}\t${rs.getString(2)}\t${rs.getString(3)}\t${rs.getLong(4)}\t${rs.getString(5)}")
            }
        }
    }
}
