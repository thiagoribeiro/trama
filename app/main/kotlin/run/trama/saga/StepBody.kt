package run.trama.saga

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * The one mapping from a step's raw response body to the value kept as its result. The executor
 * uses it for the in-memory [StepResult], and the store writes exactly its JSON form to
 * `saga_step_result`. A result therefore reads the same before and after a reload: in
 * `nodes.<id>`, in `payload._nodes.<id>` for split branches (a redelivered split rebuilds the same
 * payload either way), and after any resume.
 */
object StepBody {
    private val json = Json
    private val jsonNumber = Regex("""-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?""")

    /** Empty (or blank) → null; valid JSON → that JSON; any other text → a JSON string of it. */
    fun value(raw: String?): JsonElement? {
        if (raw.isNullOrBlank()) return null
        val parsed = parseStrict(raw)
        return parsed ?: JsonPrimitive(raw)
    }

    /** [value] as the JSON text stored in a `jsonb` column; null when there is no value. */
    fun storedJson(raw: String?): String? = value(raw)?.toString()

    /**
     * kotlinx's tree parser alone is not a validity check: it accepts bare words such as `ok`,
     * which Postgres then rejects, failing the whole write.
     */
    fun isStrictJson(raw: String): Boolean = parseStrict(raw) != null

    private fun parseStrict(raw: String): JsonElement? {
        val element = runCatching { json.parseToJsonElement(raw) }.getOrNull() ?: return null
        fun valid(e: JsonElement): Boolean = when (e) {
            is JsonNull -> true
            is JsonPrimitive -> e.isString || e.content == "true" || e.content == "false" || jsonNumber.matches(e.content)
            is JsonObject -> e.values.all(::valid)
            is JsonArray -> e.all(::valid)
        }
        return element.takeIf(::valid)
    }
}
