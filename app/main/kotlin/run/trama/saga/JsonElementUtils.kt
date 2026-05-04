package run.trama.saga

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Converts a [JsonElement] to a plain Kotlin value suitable for use in
 * template evaluation contexts and json-logic expressions.
 *
 * Mapping:
 * - null / [JsonNull]    → null
 * - [JsonPrimitive]      → String (if isString), Boolean, Long, Double, or String fallback
 * - [JsonObject]         → Map<String, Any?>
 * - [JsonArray]          → List<Any?>
 */
fun JsonElement?.toAny(): Any? = when (this) {
    null, JsonNull -> null
    is JsonPrimitive -> if (isString) content
        else content.toBooleanStrictOrNull()
            ?: content.toLongOrNull()
            ?: content.toDoubleOrNull()
            ?: content
    is JsonObject -> mapValues { (_, v) -> v.toAny() }
    is JsonArray -> map { it.toAny() }
}
