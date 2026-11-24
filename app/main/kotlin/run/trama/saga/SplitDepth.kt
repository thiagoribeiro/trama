package run.trama.saga

/**
 * Split nesting depth of an execution ([SagaExecution.splitDepth]). A root execution starts at
 * the depth its start request carries in [HEADER] (0 without it), each branch a split spawns is one
 * deeper than its parent, and every outgoing HTTP call of the execution (task nodes,
 * compensations, workflow callbacks) sends its depth in [HEADER]. A trama that receives such a call
 * as a saga start therefore keeps counting, so `split.maxDepth` bounds a whole chain of trama
 * instances calling each other, not just one definition.
 */
object SplitDepth {
    const val HEADER = "X-Trama-Split-Depth"
    const val DEFAULT_MAX = 3

    /** The depth a start request asks for: 0 without [HEADER], null when its value is not a non-negative integer. */
    fun parseHeader(value: String?): Int? {
        if (value == null) return 0
        val trimmed = value.trim()
        if (trimmed.isEmpty() || !trimmed.all { it in '0'..'9' }) return null
        return trimmed.toIntOrNull()
    }

    /** Why a split at [parentDepth] may not spawn its branches under [maxDepth], or null when it may. */
    fun exceeded(parentDepth: Int, maxDepth: Int): String? =
        if (parentDepth >= maxDepth) "split depth ${parentDepth.toLong() + 1} exceeds split.maxDepth $maxDepth" else null
}
