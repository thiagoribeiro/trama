package run.trama.telemetry

import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.io.File
import java.lang.reflect.Modifier
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution

/**
 * Guards grafana/trama-saga-dashboard.json against querying series the runtime never emits
 * (it shipped panels for metrics that did not exist). Every public record and set method on
 * [Metrics] is invoked once, so new metrics are covered automatically.
 */
class DashboardMetricsContractTest {

    private val labels = setOf("saga_name", "saga_version")

    private fun dashboardSeries(): Set<String> {
        val exprs = mutableListOf<String>()
        fun walk(e: JsonElement) {
            when (e) {
                is JsonObject -> e.forEach { (k, v) -> if (k == "expr" && v is JsonPrimitive) exprs += v.content else walk(v) }
                is JsonArray -> e.forEach(::walk)
                else -> {}
            }
        }
        walk(Json.parseToJsonElement(File("grafana/trama-saga-dashboard.json").readText()))
        return exprs.flatMap { Regex("""\bsaga_[a-z_]+\b""").findAll(it).map { m -> m.value }.toList() }
            .filterNot { it in labels }
            .toSet()
    }

    private fun emittedSeries(): Set<String> {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        val metrics = Metrics(registry)
        val execution = SagaExecution(
            definition = SagaDefinition("contract", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
            id = UUID.randomUUID(),
            startedAt = Instant.now(),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = "a"),
        )
        Metrics::class.java.declaredMethods
            .filter { Modifier.isPublic(it.modifiers) && !it.isSynthetic && (it.name.startsWith("record") || it.name.startsWith("set")) }
            .forEach { method ->
                val args = method.parameterTypes.map { type ->
                    when (type) {
                        String::class.java -> "x"
                        java.lang.Long.TYPE -> 1L
                        java.lang.Integer.TYPE -> 1
                        Instant::class.java -> Instant.now()
                        SagaExecution::class.java -> execution
                        else -> error("add a sample value for ${type.name} (${method.name})")
                    }
                }
                method.invoke(metrics, *args.toTypedArray())
            }
        return registry.scrape().lines()
            .filter { it.isNotBlank() && !it.startsWith("#") }
            .map { it.substringBefore('{').substringBefore(' ') }
            .toSet()
    }

    @Test
    fun `every series the Grafana dashboard queries is emitted by the runtime`() {
        val emitted = emittedSeries()
        val missing = dashboardSeries() - emitted
        assertTrue(missing.isEmpty(), "dashboard queries series the runtime never emits: $missing")
    }
}
