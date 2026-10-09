package run.trama.saga

import kotlinx.serialization.json.*

/** Typed replacement applies to source placeholders, never recursively to resolved values. */
internal object TypedJsonTemplate {
    private val reference = Regex("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_.\\[\\]-]*)\\s*}}")
    private val path = Regex("[A-Za-z_][A-Za-z0-9_-]*(?:\\.[A-Za-z0-9_-]+|\\[\\d+])*")

    fun render(source: String, context: Map<String, Any?>): String {
        fun resolve(element: JsonElement): JsonElement = when (element) {
            is JsonObject -> if (element.keys == setOf("__trama_literal_json__")) {
                element.getValue("__trama_literal_json__")
            } else JsonObject(element.mapValues { resolve(it.value) })
            is JsonArray -> JsonArray(element.map { resolve(it) })
            is JsonPrimitive -> if (!element.isString) element else {
                val text = element.content
                val full = reference.matchEntire(text)
                if (full != null) jsonValue(lookup(context, full.groupValues[1])) else {
                    val rendered = reference.replace(text) { match ->
                        when (val value = lookup(context, match.groupValues[1])) {
                            null -> ""
                            is Map<*, *>, is List<*> -> error("Collection reference must occupy an entire JSON value: ${match.groupValues[1]}")
                            else -> value.toString()
                        }
                    }
                    require(!reference.replace(text, "").contains("{{")) {
                        "Unsupported typed JSON template expression: $text"
                    }
                    JsonPrimitive(rendered)
                }
            }
        }
        return resolve(Json.parseToJsonElement(source)).toString()
    }

    private fun lookup(context: Map<String, Any?>, reference: String): Any? {
        require(path.matches(reference)) { "Invalid typed reference: $reference" }
        val parts = reference.replace(Regex("\\[(\\d+)]"), ".$1").split('.')
        var value: Any? = context
        for (part in parts) {
            value = when (val parent = value) {
                null -> null
                is Map<*, *> -> parent[part]
                is List<*> -> parent.getOrNull(part.toIntOrNull()
                    ?: error("List reference requires an index: $reference"))
                else -> error("Reference traverses a scalar: $reference")
            }
        }
        return value
    }

    private fun jsonValue(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is JsonElement -> value
        is String -> JsonPrimitive(value)
        is Boolean -> JsonPrimitive(value)
        is Number -> JsonPrimitive(value)
        is Map<*, *> -> JsonObject(value.entries.associate { (key, item) ->
            require(key is String) { "JSON object keys must be strings" }
            key to jsonValue(item)
        })
        is List<*> -> JsonArray(value.map { jsonValue(it) })
        else -> error("Unsupported typed JSON value: ${value.javaClass.name}")
    }
}
