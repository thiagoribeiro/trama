package run.trama.saga

import com.github.mustachejava.DefaultMustacheFactory
import com.github.mustachejava.Mustache
import run.trama.saga.store.SagaRepository
import java.io.StringReader
import java.io.StringWriter
import java.util.concurrent.ConcurrentHashMap
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

interface TemplateRenderer {
    fun render(template: TemplateString, context: Map<String, Any?>): String
}

class MustacheTemplateRenderer : TemplateRenderer {
    private val mustacheFactory = DefaultMustacheFactory()
    private val templateCache = ConcurrentHashMap<String, Mustache>()

    override fun render(template: TemplateString, context: Map<String, Any?>): String {
        val mustache = templateCache.computeIfAbsent(template.value) { key ->
            mustacheFactory.compile(StringReader(key), "saga-template")
        }
        val writer = StringWriter()
        mustache.execute(writer, context).flush()
        return writer.toString()
    }
}

object TemplateContextBuilder {
    fun build(
        execution: SagaExecution,
        stepName: String,
        phase: ExecutionPhase,
        stepResults: List<SagaRepository.StepResultForTemplate>,
        payload: Map<String, PayloadValue> = emptyMap(),
    ): Map<String, Any?> {
        val base = mutableMapOf<String, Any?>(
            "sagaId" to execution.id.toString(),
            "sagaName" to execution.definition.name,
            "sagaVersion" to execution.definition.version,
            "stepName" to stepName,
            "phase" to phase.name,
        )
        base["payload"] = payload.mapValues { jsonToAny(it.value.value) }
        val stepsList = stepResults.map { step ->
            mapOf(
                "index" to step.index,
                "name" to step.name,
                "body" to jsonToAny(step.upBody ?: step.downBody),
                "up" to mapOf("body" to jsonToAny(step.upBody)),
                "down" to mapOf("body" to jsonToAny(step.downBody)),
            )
        }
        val stepsByIndex = stepResults.associate { step ->
            step.index.toString() to mapOf(
                "index" to step.index,
                "name" to step.name,
                "body" to jsonToAny(step.upBody ?: step.downBody),
                "up" to mapOf("body" to jsonToAny(step.upBody)),
                "down" to mapOf("body" to jsonToAny(step.downBody)),
            )
        }
        base["steps"] = stepsList
        base["step"] = stepsByIndex
        return base
    }

    private fun jsonToAny(element: JsonElement?): Any? {
        return when (element) {
            null, JsonNull -> null
            is JsonPrimitive -> {
                if (element.isString) {
                    element.content
                } else {
                    element.content.toBooleanStrictOrNull()
                        ?: element.content.toLongOrNull()
                        ?: element.content.toDoubleOrNull()
                        ?: element.content
                }
            }
            is JsonObject -> element.mapValues { jsonToAny(it.value) }
            is JsonArray -> element.map { jsonToAny(it) }
        }
    }
}
