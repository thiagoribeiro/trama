package run.trama.saga.workflow

import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode

object WorkflowDefinitionValidator {

    fun validate(definition: SagaDefinitionV2): List<String> {
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
                val outgoing: List<String> = when (node) {
                    is NodeDefinition.Task -> listOfNotNull(node.next)
                    is NodeDefinition.Switch -> node.cases.map { it.target } + node.default
                    is NodeDefinition.Sleep -> listOfNotNull(node.next)
                    is NodeDefinition.Split -> node.branches
                    is NodeDefinition.Join -> listOfNotNull(node.next)
                }
                for (target in outgoing) {
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

        return errors
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
            val neighbors = when (val node = nodeMap[nodeId]) {
                is NodeDefinition.Task -> listOfNotNull(node.next)
                is NodeDefinition.Switch -> node.cases.map { it.target } + node.default
                is NodeDefinition.Sleep -> listOfNotNull(node.next)
                is NodeDefinition.Split -> node.branches + node.join
                is NodeDefinition.Join -> listOfNotNull(node.next)
                null -> emptyList()
            }
            val cycle = neighbors.any { dfs(it) }
            inStack -= nodeId
            return cycle
        }

        return dfs(definition.entrypoint)
    }
}
