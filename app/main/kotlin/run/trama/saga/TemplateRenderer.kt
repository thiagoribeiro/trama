package run.trama.saga

import com.github.mustachejava.DefaultMustacheFactory
import com.github.mustachejava.Mustache
import java.io.StringReader
import java.io.StringWriter
import java.util.concurrent.ConcurrentHashMap
import kotlinx.serialization.json.JsonElement

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
        stepResults: List<StepResult>,
        payload: Map<String, PayloadValue> = emptyMap(),
    ): Map<String, Any?> {
        val base = mutableMapOf<String, Any?>(
            "sagaId" to execution.id.toString(),
            "sagaName" to execution.definition.name,
            "sagaVersion" to execution.definition.version,
            "stepName" to stepName,
            "phase" to phase.name,
        )
        base["payload"] = payload.mapValues { it.value.value.toAny() }
        fun stepEntry(step: StepResult): Map<String, Any?> = mapOf(
            "index" to step.index,
            "name"  to step.name,
            "body"  to (step.upBody ?: step.downBody).toAny(),
            "up"    to mapOf("body" to step.upBody.toAny()),
            "down"  to mapOf("body" to step.downBody.toAny()),
        )
        base["steps"] = stepResults.map { stepEntry(it) }
        base["step"] =
            stepResults.associate { it.index.toString() to stepEntry(it) } +
            stepResults.associate { it.name to stepEntry(it) }
        // nodes.<name>.response.body — keyed by node name
        base["nodes"] = stepResults.associate { step ->
            step.name to mapOf(
                "response" to mapOf(
                    "body" to (step.upBody ?: step.downBody).toAny(),
                ),
            )
        }
        base["prev"] = stepResults.lastOrNull()?.let {
            mapOf("body" to (it.upBody ?: it.downBody).toAny())
        }
        return base
    }
}
