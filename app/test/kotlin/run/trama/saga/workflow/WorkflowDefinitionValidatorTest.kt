package run.trama.saga.workflow

import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkflowDefinitionValidatorTest {

    private val failureHandling = FailureHandling.Retry(maxAttempts = 3, delayMillis = 1000)

    private fun taskAction() = NodeActionDef(
        mode = TaskMode.SYNC,
        request = HttpCall(url = TemplateString("http://service/action"), verb = HttpVerb.POST),
    )

    private fun compensation() = HttpCall(
        url = TemplateString("http://service/compensate"),
        verb = HttpVerb.DELETE,
    )

    @Test
    fun `cyclic workflow without compensation is valid`() {
        val definition = SagaDefinitionV2(
            name = "cyclic-flow",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "step-a",
            nodes = listOf(
                NodeDefinition.Task(id = "step-a", action = taskAction(), next = "step-b"),
                NodeDefinition.Task(id = "step-b", action = taskAction(), next = "step-a"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.isEmpty(), "Expected no errors for cyclic workflow without compensation, got: $errors")
    }

    @Test
    fun `cyclic workflow with compensation is rejected`() {
        val definition = SagaDefinitionV2(
            name = "cyclic-with-compensation",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "step-a",
            nodes = listOf(
                NodeDefinition.Task(id = "step-a", action = taskAction(), compensation = compensation(), next = "step-b"),
                NodeDefinition.Task(id = "step-b", action = taskAction(), next = "step-a"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(
            errors.any { it.contains("compensation is not allowed in cyclic workflows") },
            "Expected compensation-in-cycle error, got: $errors",
        )
    }

    @Test
    fun `non-cyclic workflow with compensation is valid`() {
        val definition = SagaDefinitionV2(
            name = "linear-with-compensation",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "step-a",
            nodes = listOf(
                NodeDefinition.Task(id = "step-a", action = taskAction(), compensation = compensation(), next = "step-b"),
                NodeDefinition.Task(id = "step-b", action = taskAction(), compensation = compensation()),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.isEmpty(), "Expected no errors for non-cyclic workflow with compensation, got: $errors")
    }

    @Test
    fun `cyclic workflow via switch default is detected`() {
        val definition = SagaDefinitionV2(
            name = "switch-cycle",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "step-a",
            nodes = listOf(
                NodeDefinition.Task(id = "step-a", action = taskAction(), compensation = compensation(), next = "router"),
                NodeDefinition.Switch(
                    id = "router",
                    cases = listOf(run.trama.saga.SwitchCaseDef(
                        name = "done",
                        whenExpression = kotlinx.serialization.json.JsonObject(emptyMap()),
                        target = "step-b",
                    )),
                    default = "step-a",
                ),
                NodeDefinition.Task(id = "step-b", action = taskAction()),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(
            errors.any { it.contains("compensation is not allowed in cyclic workflows") },
            "Expected compensation-in-cycle error for switch-driven cycle, got: $errors",
        )
    }
}
