package run.trama.cli

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.jsonObject
import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode
import run.trama.saga.workflow.WorkflowDefinitionValidator
import java.io.File
import kotlin.system.exitProcess

// ─── JSON instance ────────────────────────────────────────────────────────────

private val json = Json {
    ignoreUnknownKeys = true
    explicitNulls     = false
    encodeDefaults    = true
}

// ─── Entry point ──────────────────────────────────────────────────────────────

fun main(args: Array<String>) {
    // Silence jOOQ — its static initializers may run when kotlinx-serialization
    // loads classes that transitively touch the jOOQ module.
    System.setProperty("org.jooq.no-logo", "true")
    System.setProperty("org.jooq.no-tips", "true")

    val argList      = args.toMutableList()
    val validateOnly = argList.remove("--validate-only")
    val definitionPath = argList.getOrNull(0)
    val scenarioPath   = argList.getOrNull(1)

    if (definitionPath == null) {
        System.err.println("Usage: trama-validate <definition.json> [scenario.json] [--validate-only]")
        System.err.println()
        System.err.println("  definition.json  — v2 saga definition (must contain 'nodes')")
        System.err.println("  scenario.json    — mock responses for each node (optional)")
        System.err.println("  --validate-only  — skip execution simulation even if scenario provided")
        exitProcess(1)
    }

    // ── Read definition ───────────────────────────────────────────────────────

    val definitionFile = File(definitionPath)
    if (!definitionFile.exists()) {
        System.err.println("Error: definition file not found: $definitionPath")
        exitProcess(1)
    }

    val defObj = runCatching {
        json.parseToJsonElement(definitionFile.readText()).jsonObject
    }.getOrElse {
        System.err.println("Error: definition is not valid JSON — ${it.message}")
        exitProcess(1)
    }

    if (!defObj.containsKey("nodes")) {
        System.err.println("Error: only v2 definitions (containing 'nodes') are supported.")
        System.err.println("       v1 step-based definitions are not supported by this tool.")
        exitProcess(1)
    }

    val definition = runCatching {
        json.decodeFromJsonElement(SagaDefinitionV2.serializer(), defObj)
    }.getOrElse {
        System.err.println("Error: failed to deserialize definition — ${it.message}")
        exitProcess(1)
    }

    // ── Banner ────────────────────────────────────────────────────────────────

    val divider = "━".repeat(60)
    println()
    println(divider)
    println("  trama validate  ·  ${definition.name} / ${definition.version}")
    println(divider)
    println()

    // ── Step 1: structural validation ─────────────────────────────────────────

    val totalSteps = if (!validateOnly && scenarioPath != null) 2 else 1
    println("[1/$totalSteps] Structural validation")

    val errors = WorkflowDefinitionValidator.validate(definition)
    if (errors.isNotEmpty()) {
        errors.forEach { println("  ✗ $it") }
        println()
        println("${errors.size} error(s) found. Fix them before running a simulation.")
        exitProcess(1)
    }

    val taskCount  = definition.nodes.count { it is NodeDefinition.Task }
    val asyncCount = definition.nodes.count { it is NodeDefinition.Task && it.action.mode == TaskMode.ASYNC }
    val switchCount = definition.nodes.count { it is NodeDefinition.Switch }
    println("  ✓ ${definition.nodes.size} nodes  ($taskCount task, $switchCount switch, $asyncCount async)")
    println("  ✓ all node references resolve")
    println()

    // ── No scenario: stop here ────────────────────────────────────────────────

    if (validateOnly || scenarioPath == null) {
        if (scenarioPath == null && !validateOnly) {
            println("Tip: pass a scenario file to also simulate the execution.")
            println("     trama-validate $definitionPath scenario.json")
            println()
        }
        println("OK")
        exitProcess(0)
    }

    // ── Step 2: execution simulation ──────────────────────────────────────────

    val scenarioFile = File(scenarioPath)
    if (!scenarioFile.exists()) {
        System.err.println("Error: scenario file not found: $scenarioPath")
        exitProcess(1)
    }

    val scenario = runCatching {
        json.decodeFromString(DryRunScenario.serializer(), scenarioFile.readText())
    }.getOrElse {
        System.err.println("Error: failed to parse scenario — ${it.message}")
        exitProcess(1)
    }

    val payloadSummary = scenario.payload.entries
        .take(4)
        .joinToString(", ") { (k, v) -> "$k=${v.toString().trim('"')}" }
    println("[2/2] Execution simulation")
    println("  payload: {$payloadSummary}")
    println()

    val result = DryRunSimulator().run(definition, scenario)
    printTrace(result)

    println()
    if (result.outcome == SimOutcome.SUCCEEDED) {
        println("All checks passed.")
        exitProcess(0)
    } else {
        println("Simulation failed.")
        exitProcess(1)
    }
}

// ─── Trace output ─────────────────────────────────────────────────────────────

private fun printTrace(result: SimulationResult) {
    for (entry in result.entries) {
        when (entry) {
            is TraceEntry.Task -> {
                println("  → ${entry.nodeId.padEnd(20)} [${if (entry.success) "sync" else "sync ✗"}]")
                println("      ${entry.verb.padEnd(7)} ${entry.renderedUrl}")
                if (!entry.renderedBody.isNullOrBlank() && entry.renderedBody != "{}") {
                    println("      body    ${compact(entry.renderedBody)}")
                }
                val mark = if (entry.success) "✓" else "✗"
                println("      ← ${entry.responseStatus}  $mark  ${compact(entry.responseBody)}")
                println()
            }

            is TraceEntry.AsyncTask -> {
                println("  → ${entry.nodeId.padEnd(20)} [async]")
                println("      ${entry.verb.padEnd(7)} ${entry.renderedUrl}")
                if (!entry.renderedBody.isNullOrBlank() && entry.renderedBody != "{}") {
                    println("      body    ${compact(entry.renderedBody)}")
                }
                println("      ← 202   accepted (execution pauses here in production)")
                println("      callback ← ${compact(entry.callbackBody)}")
                println()
            }

            is TraceEntry.Switch -> {
                println("  → ${entry.nodeId.padEnd(20)} [switch]")
                val routing = if (entry.usedDefault)
                    "no case matched, used default  →  ${entry.targetNodeId}"
                else
                    "matched \"${entry.matchedCase}\"  →  ${entry.targetNodeId}"
                println("      $routing")
                println()
            }
        }
    }

    when (result.outcome) {
        SimOutcome.SUCCEEDED ->
            println("  ✓ SUCCEEDED")
        SimOutcome.FAILED -> {
            val where  = result.failureNodeId?.let { " at '$it'" } ?: ""
            val status = result.failureStatus?.let { " (HTTP $it)" } ?: ""
            println("  ✗ FAILED$where$status")
        }
        SimOutcome.MAX_NODES_EXCEEDED ->
            println("  ✗ MAX NODES EXCEEDED — possible cycle in the definition")
    }
}

/** Compact-encode a string if it is valid JSON, otherwise return as-is. */
private fun compact(s: String): String = runCatching {
    Json.encodeToString(JsonElement.serializer(), Json.parseToJsonElement(s))
}.getOrDefault(s)
