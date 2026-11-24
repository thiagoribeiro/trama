package run.trama.saga.workflow

import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SplitDepth
import run.trama.saga.TaskMode

object WorkflowDefinitionValidator {

    /** [maxSplitDepth]: `split.maxDepth`; a definition whose splits nest deeper is refused. */
    fun validate(definition: SagaDefinitionV2, maxSplitDepth: Int = SplitDepth.DEFAULT_MAX): List<String> {
        val errors = mutableListOf<String>()

        if (definition.name.isBlank()) errors.add("name must not be blank")
        if (definition.version.isBlank()) errors.add("version must not be blank")
        if (definition.nodes.isEmpty()) errors.add("nodes must not be empty")

        val nodeIds = definition.nodes.map { it.id }.toSet()
        val nodeMap = definition.nodes.associateBy { it.id }

        // Duplicate ids
        if (nodeIds.size != definition.nodes.size) {
            errors.add("node ids must be unique")
        }

        // Entrypoint must exist
        if (definition.entrypoint.isBlank()) {
            errors.add("entrypoint must not be blank")
        } else if (definition.entrypoint !in nodeIds) {
            errors.add("entrypoint '${definition.entrypoint}' does not reference an existing node")
        }

        definition.nodes.forEachIndexed { index, node ->
            val prefix = "nodes[$index](id=${node.id})"

            if (node.id.isBlank()) {
                errors.add("$prefix.id must not be blank")
            }

            when (node) {
                is NodeDefinition.Task -> {
                    val action = node.action

                    if (action.request.url.value.isBlank()) {
                        errors.add("$prefix.action.request.url must not be blank")
                    }

                    // next reference must exist (null = terminal is allowed)
                    val next = node.next
                    if (next != null && next !in nodeIds) {
                        errors.add("$prefix.next '$next' does not reference an existing node")
                    }

                    if (action.mode == TaskMode.ASYNC) {
                        if (action.callback == null) {
                            errors.add("$prefix.action.callback is required when mode is async")
                        } else if (action.callback.timeoutMillis <= 0) {
                            errors.add("$prefix.action.callback.timeoutMillis must be positive")
                        }
                        if (action.acceptedStatusCodes != null && action.acceptedStatusCodes.isEmpty()) {
                            errors.add("$prefix.action.acceptedStatusCodes must not be empty when provided")
                        }
                    }
                }

                is NodeDefinition.Switch -> {
                    if (node.cases.isEmpty()) {
                        errors.add("$prefix.cases must not be empty")
                    }

                    node.cases.forEachIndexed { caseIndex, case ->
                        val casePrefix = "$prefix.cases[$caseIndex]"
                        if (case.target.isBlank()) {
                            errors.add("$casePrefix.target must not be blank")
                        } else if (case.target !in nodeIds) {
                            errors.add("$casePrefix.target '${case.target}' does not reference an existing node")
                        }
                    }

                    if (node.default.isBlank()) {
                        errors.add("$prefix.default must not be blank")
                    } else if (node.default !in nodeIds) {
                        errors.add("$prefix.default '${node.default}' does not reference an existing node")
                    }
                }

                is NodeDefinition.Sleep -> {
                    if (node.durationMillis <= 0) {
                        errors.add("$prefix.durationMillis must be positive")
                    }
                    val next = node.next
                    if (next != null && next !in nodeIds) {
                        errors.add("$prefix.next '$next' does not reference an existing node")
                    }
                }

                is NodeDefinition.Split -> {
                    if (node.branches.size < 2) {
                        errors.add("$prefix.branches must contain at least 2 entries")
                    }
                    node.branches.forEachIndexed { branchIndex, branchId ->
                        if (branchId.isBlank()) {
                            errors.add("$prefix.branches[$branchIndex] must not be blank")
                        } else if (branchId !in nodeIds) {
                            errors.add("$prefix.branches contains unknown node '$branchId'")
                        }
                    }
                    if (node.join.isBlank()) {
                        errors.add("$prefix.join must not be blank")
                    } else if (node.join !in nodeIds) {
                        errors.add("$prefix.join '${node.join}' does not reference an existing node")
                    } else if (nodeMap[node.join] !is NodeDefinition.Join) {
                        errors.add("$prefix.join '${node.join}' must reference a join node")
                    }
                }

                is NodeDefinition.Join -> {
                    val next = node.next
                    if (next != null && next !in nodeIds) {
                        errors.add("$prefix.next '$next' does not reference an existing node")
                    }
                }
            }
        }

        // Each join must be owned by exactly one split.
        definition.nodes.filterIsInstance<NodeDefinition.Split>()
            .groupBy { it.join }
            .filter { (_, owners) -> owners.size > 1 }
            .forEach { (joinId, owners) ->
                errors.add(
                    "join '$joinId' must be owned by exactly one split node, " +
                        "but is referenced by splits: ${owners.joinToString { it.id }}",
                )
            }

        // Only the owning split may route into a join node; every other edge that
        // targets a join id is a "leak" into the barrier from outside its split.
        val joinIds = definition.nodes.filterIsInstance<NodeDefinition.Join>().map { it.id }.toSet()
        if (joinIds.isNotEmpty()) {
            definition.nodes.forEach { node ->
                for (target in node.successors { it.branches }) {
                    val isOwnJoinEdge = node is NodeDefinition.Split && node.join == target
                    if (target in joinIds && !isOwnJoinEdge) {
                        errors.add("node '${node.id}' must not reference join '$target' directly; only its owning split may")
                    }
                }
            }
        }

        definition.onSuccessCallback?.let { cb ->
            if (cb.url.value.isBlank()) errors.add("onSuccessCallback.url must not be blank")
        }
        definition.onFailureCallback?.let { cb ->
            if (cb.url.value.isBlank()) errors.add("onFailureCallback.url must not be blank")
        }

        if (hasCycle(definition) && definition.nodes.any { it is NodeDefinition.Task && it.compensation != null }) {
            errors.add("compensation is not allowed in cyclic workflows")
        }

        if (definition.entrypoint in nodeMap) {
            val depth = splitNesting(nodeMap, definition.entrypoint)
            if (depth == null) errors.add("a split is reachable from its own branches, so its nesting is unbounded")
            else if (depth > maxSplitDepth) errors.add("split nesting depth $depth exceeds split.maxDepth $maxSplitDepth")
        }

        return errors
    }

    /**
     * Deepest split nesting one execution walking from [start] reaches: a split's branches are one
     * level deeper, and the walk goes on at its join. Null when a split is reachable from its own
     * branches ([enclosing]), which nests without bound. Counts from 0: the depth a start request
     * brings in `X-Trama-Split-Depth` is enforced at run time, by the split itself.
     */
    private fun splitNesting(nodes: Map<String, NodeDefinition>, start: String, enclosing: Set<String> = emptySet()): Int? {
        var deepest = 0
        val seen = mutableSetOf<String>()
        val pending = ArrayDeque(listOf(start))
        while (pending.isNotEmpty()) {
            val node = nodes[pending.removeLast()] ?: continue
            if (!seen.add(node.id)) continue
            // One execution goes on at a split's join; its branches are other executions, one deeper.
            pending += node.successors { split ->
                if (split.id in enclosing) return null
                val inner = split.branches.maxOfOrNull { splitNesting(nodes, it, enclosing + split.id) ?: return null } ?: 0
                deepest = maxOf(deepest, inner + 1)
                listOf(split.join)
            }
        }
        return deepest
    }

    private fun hasCycle(definition: SagaDefinitionV2): Boolean {
        val nodeMap = definition.nodes.associateBy { it.id }
        val visited = mutableSetOf<String>()
        val inStack = mutableSetOf<String>()

        fun dfs(nodeId: String): Boolean {
            if (nodeId in inStack) return true
            if (nodeId in visited) return false
            inStack += nodeId
            visited += nodeId
            val neighbors = nodeMap[nodeId]?.successors { it.branches + it.join }.orEmpty()
            val cycle = neighbors.any { dfs(it) }
            inStack -= nodeId
            return cycle
        }

        return dfs(definition.entrypoint)
    }

    /** The nodes [this] leads to; a split's depend on the check, so [split] gives them. */
    private inline fun NodeDefinition.successors(split: (NodeDefinition.Split) -> List<String>): List<String> = when (this) {
        is NodeDefinition.Task -> listOfNotNull(next)
        is NodeDefinition.Sleep -> listOfNotNull(next)
        is NodeDefinition.Join -> listOfNotNull(next)
        is NodeDefinition.Switch -> cases.map { it.target } + default
        is NodeDefinition.Split -> split(this)
    }
}
