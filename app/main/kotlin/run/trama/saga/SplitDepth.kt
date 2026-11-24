package run.trama.saga

/**
 * Split nesting depth of an execution ([SagaExecution.splitDepth]), counted across trama instances:
 * a run starts at the depth in its start request's [HEADER] (0 without it), each split branch is
 * one deeper than its parent, and every outgoing call sends the execution's depth in [HEADER].
 */
object SplitDepth {
    const val HEADER = "X-Trama-Split-Depth"

    /** The start depth a request asks for: 0 without [HEADER], null when it is not a non-negative integer. */
    fun parseHeader(value: String?): Int? =
        if (value == null) 0 else value.trim().toIntOrNull()?.takeIf { it >= 0 }
}
