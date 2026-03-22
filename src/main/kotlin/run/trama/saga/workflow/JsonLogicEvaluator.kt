package run.trama.saga.workflow

import io.github.jamsesso.jsonlogic.JsonLogic
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

object JsonLogicEvaluator {
    private val jsonLogic = JsonLogic()
    private val json = Json

    /**
     * Evaluates a json-logic [expression] against [data] and returns a boolean.
     * Returns false on any evaluation error.
     */
    fun evaluateBool(expression: JsonElement, data: Map<String, Any?>): Boolean {
        val ruleStr = json.encodeToString(JsonElement.serializer(), expression)
        return try {
            when (val result = jsonLogic.apply(ruleStr, data)) {
                is Boolean -> result
                is Number -> result.toDouble() != 0.0
                is String -> result.isNotEmpty()
                null -> false
                else -> true
            }
        } catch (_: Exception) {
            false
        }
    }
}
