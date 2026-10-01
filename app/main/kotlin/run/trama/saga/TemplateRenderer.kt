package run.trama.saga

import com.github.mustachejava.DefaultMustacheFactory
import com.github.mustachejava.Mustache
import java.io.StringReader
import java.io.StringWriter
import java.io.Writer
import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import kotlinx.serialization.json.JsonElement

/**
 * How `{{ }}` values are escaped, chosen by where the rendered text ends up. `{{{ }}}` (and
 * `{{& }}`) always write the raw value. Mustache's default HTML escaping is only right for
 * XML/HTML bodies: in a JSON body it corrupted values (`'` → `&#39;`) and let `\` or newlines
 * through, producing invalid JSON; in URLs and headers it mangled `&`, `=`, quotes.
 */
enum class TemplateEscaping {
    /** Literal value (URLs: templates may build whole URLs/paths, so nothing is encoded). */
    NONE,
    /** Literal value with CR/LF removed, so a value can never split a header. */
    HEADER_VALUE,
    /** Escaped for inclusion inside a JSON string literal. */
    JSON_STRING,
    /** XML/HTML entity escaping (the previous behavior, correct for XML bodies). */
    XML,
    /** application/x-www-form-urlencoded value encoding. */
    FORM_URLENCODED,
    ;

    companion object {
        /** Escaping for a request body, from its Content-Type header (or its shape when absent). */
        fun forBody(call: HttpCall): TemplateEscaping {
            val contentType = call.headers.entries
                .firstOrNull { it.key.equals("Content-Type", ignoreCase = true) }
                ?.value?.value?.lowercase()
            return when {
                contentType == null -> {
                    val start = call.body?.value?.trimStart()?.firstOrNull()
                    if (start == '{' || start == '[') JSON_STRING else NONE
                }
                "json" in contentType -> JSON_STRING
                "xml" in contentType || "html" in contentType -> XML
                "x-www-form-urlencoded" in contentType -> FORM_URLENCODED
                else -> NONE
            }
        }
    }
}

interface TemplateRenderer {
    fun render(template: TemplateString, context: Map<String, Any?>, escaping: TemplateEscaping): String
}

class MustacheTemplateRenderer : TemplateRenderer {
    /** A compiled template keeps a reference to its factory's encoder, so each escaping has its own. */
    private class EscapingFactory(private val escaping: TemplateEscaping) : DefaultMustacheFactory() {
        val templates = ConcurrentHashMap<String, Mustache>()

        override fun encode(value: String, writer: Writer) {
            when (escaping) {
                TemplateEscaping.NONE -> writer.write(value)
                TemplateEscaping.HEADER_VALUE -> value.forEach { if (it != '\r' && it != '\n') writer.write(it.code) }
                TemplateEscaping.JSON_STRING -> writeJsonStringContent(value, writer)
                TemplateEscaping.XML -> super.encode(value, writer)
                TemplateEscaping.FORM_URLENCODED -> writer.write(URLEncoder.encode(value, Charsets.UTF_8))
            }
        }
    }

    private val factories = TemplateEscaping.entries.associateWith { EscapingFactory(it) }

    override fun render(template: TemplateString, context: Map<String, Any?>, escaping: TemplateEscaping): String {
        val factory = factories.getValue(escaping)
        val mustache = factory.templates.computeIfAbsent(template.value) { key ->
            factory.compile(StringReader(key), "saga-template")
        }
        val writer = StringWriter()
        mustache.execute(writer, context).flush()
        return writer.toString()
    }

    private companion object {
        fun writeJsonStringContent(value: String, writer: Writer) {
            for (c in value) {
                when (c) {
                    '"' -> writer.write("\\\"")
                    '\\' -> writer.write("\\\\")
                    '\n' -> writer.write("\\n")
                    '\r' -> writer.write("\\r")
                    '\t' -> writer.write("\\t")
                    '\b' -> writer.write("\\b")
                    '\u000C' -> writer.write("\\f")
                    else -> if (c < ' ' || c == ' ' || c == ' ') {
                        writer.write("\\u%04x".format(c.code))
                    } else {
                        writer.write(c.code)
                    }
                }
            }
        }
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
        base["input"] = base["payload"] // alias, as in switch and callback conditions
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
