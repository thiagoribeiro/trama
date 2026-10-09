package run.trama.saga

import kotlinx.serialization.json.*

/** Typed replacement applies to source placeholders, never recursively to resolved values. */
internal object TypedJsonTemplate {
    private val reference = Regex("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_.\\[\\]-]*)\\s*}}")
    private val path = Regex("[A-Za-z_][A-Za-z0-9_-]*(?:\\.[A-Za-z0-9_-]+|\\[\\d+])*")

    fun render(source: String, context: Map<String, Any?>): String {
        fun resolve(element: JsonElement, location: String): JsonElement = when (element) {
            is JsonObject -> if (element.keys == setOf("__trama_literal_json__")) {
                element.getValue("__trama_literal_json__")
            } else JsonObject(element.mapValues { (key, value) -> resolve(value, "$location.$key") })
            is JsonArray -> JsonArray(element.mapIndexed { index, value -> resolve(value, "$location[$index]") })
            is JsonPrimitive -> if (!element.isString) element else {
                val text = element.content
                require(!text.contains("{{{") && !reference.replace(text, "").contains("{{")) {
                    "JSON body $location: unsupported template expression: $text"
                }
                val full = reference.matchEntire(text)
                if (full != null) jsonValue(lookup(context, full.groupValues[1], location)) else {
                    val rendered = reference.replace(text) { match ->
                        when (val value = lookup(context, match.groupValues[1], location)) {
                            null -> ""
                            is Map<*, *>, is List<*> -> error("JSON body $location: collection reference must occupy an entire JSON value: ${match.groupValues[1]}")
                            else -> value.toString()
                        }
                    }
                    JsonPrimitive(rendered)
                }
            }
        }
        return resolve(Json.parseToJsonElement(source), "$").toString()
    }

    private fun lookup(context: Map<String, Any?>, reference: String, location: String): Any? {
        val diagnostic = "JSON body $location: reference '$reference'"
        require(path.matches(reference)) { "$diagnostic has invalid syntax" }
        val parts = reference.replace(Regex("\\[(\\d+)]"), ".$1").split('.')
        var value: Any? = context
        for (part in parts) {
            value = when (val parent = value) {
                null -> error("$diagnostic traverses null at '$part'")
                is Map<*, *> -> {
                    require(parent.containsKey(part)) { "$diagnostic is missing key '$part'" }
                    parent[part]
                }
                is List<*> -> {
                    val index = part.toIntOrNull() ?: error("$diagnostic requires a list index at '$part'")
                    require(index in parent.indices) { "$diagnostic has an out-of-range index '$part'" }
                    parent[index]
                }
                else -> error("$diagnostic traverses a scalar at '$part'")
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
