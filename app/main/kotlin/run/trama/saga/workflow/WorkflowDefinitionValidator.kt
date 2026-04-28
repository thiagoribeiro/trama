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
                null -> emptyList()
            }
            val cycle = neighbors.any { dfs(it) }
            inStack -= nodeId
            return cycle
        }

        return dfs(definition.entrypoint)
    }
}
