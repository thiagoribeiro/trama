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

    // ── split / join ─────────────────────────────────────────────────────────

    @Test
    fun `valid split and join definition passes validation`() {
        val definition = SagaDefinitionV2(
            name = "split-join-flow",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
                NodeDefinition.Join(id = "fan-in", next = "after-join"),
                NodeDefinition.Task(id = "after-join", action = taskAction()),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.isEmpty(), "Expected no errors, got: $errors")
    }

    @Test
    fun `split with fewer than two branches is rejected`() {
        val definition = SagaDefinitionV2(
            name = "single-branch-split",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Join(id = "fan-in"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("branches") }, "Expected branches-count error, got: $errors")
    }

    @Test
    fun `split branch referencing unknown node produces error`() {
        val definition = SagaDefinitionV2(
            name = "bad-branch",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "ghost"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Join(id = "fan-in"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("ghost") }, "Expected unknown branch error, got: $errors")
    }

    @Test
    fun `split join referencing unknown node produces error`() {
        val definition = SagaDefinitionV2(
            name = "bad-join-ref",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "ghost-join"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("ghost-join") }, "Expected unknown join error, got: $errors")
    }

    @Test
    fun `split join must reference an actual join node`() {
        val definition = SagaDefinitionV2(
            name = "join-wrong-kind",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "branch-a"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("join") }, "Expected join-must-be-join-node error, got: $errors")
    }

    @Test
    fun `two splits cannot share the same join`() {
        val definition = SagaDefinitionV2(
            name = "shared-join",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out-1",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out-1", branches = listOf("branch-a", "branch-a2"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-a2", action = taskAction()),
                NodeDefinition.Split(id = "fan-out-2", branches = listOf("branch-b", "branch-b2"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
                NodeDefinition.Task(id = "branch-b2", action = taskAction()),
                NodeDefinition.Join(id = "fan-in"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(
            errors.any { it.contains("fan-in") && it.contains("split") },
            "Expected duplicate join-owner error, got: $errors",
        )
    }

    @Test
    fun `only the owning split may reference a join node`() {
        val definition = SagaDefinitionV2(
            name = "leaky-join",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "step-a",
            nodes = listOf(
                NodeDefinition.Task(id = "step-a", action = taskAction(), next = "fan-in"),
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
                NodeDefinition.Join(id = "fan-in"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("fan-in") }, "Expected leak-into-join error, got: $errors")
    }

    @Test
    fun `join next referencing unknown node produces error`() {
        val definition = SagaDefinitionV2(
            name = "bad-join-next",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "fan-out",
            nodes = listOf(
                NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "fan-in"),
                NodeDefinition.Task(id = "branch-a", action = taskAction()),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
                NodeDefinition.Join(id = "fan-in", next = "ghost"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.any { it.contains("ghost") }, "Expected unknown join.next error, got: $errors")
    }

    @Test
    fun `nested split inside a branch is valid`() {
        val definition = SagaDefinitionV2(
            name = "nested-split",
            version = "1",
            failureHandling = failureHandling,
            entrypoint = "outer-split",
            nodes = listOf(
                NodeDefinition.Split(id = "outer-split", branches = listOf("inner-split", "branch-b"), join = "outer-join"),
                NodeDefinition.Split(id = "inner-split", branches = listOf("branch-a1", "branch-a2"), join = "inner-join"),
                NodeDefinition.Task(id = "branch-a1", action = taskAction()),
                NodeDefinition.Task(id = "branch-a2", action = taskAction()),
                NodeDefinition.Join(id = "inner-join"),
                NodeDefinition.Task(id = "branch-b", action = taskAction()),
                NodeDefinition.Join(id = "outer-join"),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(definition)
        assertTrue(errors.isEmpty(), "Expected no errors for nested split, got: $errors")
    }
}
