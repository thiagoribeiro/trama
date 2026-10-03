package run.trama.loadtest

import kotlinx.serialization.json.Json

/**
 * Validation harness entry point. See loadtest/README.md.
 *
 *   mock     downstream service: sync/async/undo endpoints, records every call
 *   drive    submits workflows (inline runs) and writes runs.csv
 *   collect  samples process, Redis, Postgres and queue metrics into a CSV
 *   check    waits for the submitted runs to settle and verifies outcomes and invariants
 *   park     creates executions parked in each state (durability matrix, scenario 1c)
 *   pgtop    prints the top Postgres statements (pg_stat_statements)
 */
fun main(args: Array<String>) {
    val command = args.firstOrNull() ?: error("usage: <mock|drive|collect|check|park|pgtop> [--key=value ...]")
    val opts = Opts(args.drop(1))
    when (command) {
        "mock" -> runMock(opts)
        "drive" -> runDriver(opts)
        "collect" -> runCollector(opts)
        "check" -> runChecker(opts)
        "park" -> runPark(opts)
        "pgtop" -> runPgTop(opts)
        else -> error("unknown command: $command")
    }
}

class Opts(args: List<String>) {
    private val values = args.filter { it.startsWith("--") }.associate {
        val (k, v) = it.removePrefix("--").split("=", limit = 2).let { p -> p[0] to p.getOrElse(1) { "true" } }
        k to v
    }

    fun str(key: String, default: String) = values[key] ?: default
    fun int(key: String, default: Int) = values[key]?.toInt() ?: default
    fun long(key: String, default: Long) = values[key]?.toLong() ?: default
    fun double(key: String, default: Double) = values[key]?.toDouble() ?: default
}

val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

/** Defaults shared by the commands and the loadtest shell scripts. */
object Endpoints {
    const val API = "http://127.0.0.1:9100"
    const val MOCK = "http://127.0.0.1:7070"
    /** Direct (not through Toxiproxy) so collection and checking survive injected faults. */
    const val DB = "jdbc:postgresql://127.0.0.1:55433/saga"
    const val REDIS = "redis://127.0.0.1:56380"
}

fun log(msg: String) = println("[${java.time.LocalTime.now().withNano(0)}] $msg")
