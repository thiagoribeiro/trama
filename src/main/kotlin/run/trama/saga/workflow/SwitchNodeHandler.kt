package run.trama.saga.workflow

import run.trama.saga.PayloadValue
import run.trama.saga.SagaExecution
import run.trama.saga.StepResult
import run.trama.saga.toAny

/**
 * Evaluates a [SwitchNode] by running each case's json-logic expression against
 * a context built from [payload] and prior node outputs ([stepResults]).
 */
object SwitchNodeHandler {

    data class EvaluationResult(
        val targetNodeId: String,
        val matchedCaseName: String?,
        /** true when no case matched and [SwitchNode.defaultTarget] was used */
        val usedDefault: Boolean,
    )

    fun evaluate(
        node: SwitchNode,
        execution: SagaExecution,
        payload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
    ): EvaluationResult {
        val data = buildEvaluationContext(payload, stepResults)

        for (case in node.cases) {
            if (JsonLogicEvaluator.evaluateBool(case.whenExpression, data)) {
                return EvaluationResult(
                    targetNodeId = case.target,
                    matchedCaseName = case.name,
                    usedDefault = false,
                )
            }
        }

        return EvaluationResult(
            targetNodeId = node.defaultTarget,
            matchedCaseName = null,
            usedDefault = true,
        )
    }

    private fun buildEvaluationContext(
        payload: Map<String, PayloadValue>,
        stepResults: List<StepResult>,
    ): Map<String, Any?> {
        val inputMap = payload.mapValues { it.value.value.toAny() }
        val nodesMap = stepResults.associate { step ->
            step.name to mapOf(
                "response" to mapOf(
                    "body" to (step.upBody ?: step.downBody).toAny(),
                ),
            )
        }
        return mapOf(
            "input" to inputMap,
            "nodes" to nodesMap,
        )
    }
}
