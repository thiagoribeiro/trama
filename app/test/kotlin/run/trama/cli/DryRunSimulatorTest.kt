package run.trama.cli

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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

class DryRunSimulatorTest {

    private fun httpCall(url: String) = HttpCall(TemplateString(url), HttpVerb.POST)

    private fun splitJoinDefinition(): SagaDefinitionV2 = SagaDefinitionV2(
        name = "fan-out-flow",
        version = "1",
        failureHandling = FailureHandling.Retry(1, 0),
        entrypoint = "fan-out",
        nodes = listOf(
            NodeDefinition.Split(id = "fan-out", branches = listOf("branch-a", "branch-b"), join = "fan-in"),
            NodeDefinition.Task("branch-a", NodeActionDef(TaskMode.SYNC, httpCall("http://a"))),
            NodeDefinition.Task("branch-b", NodeActionDef(TaskMode.SYNC, httpCall("http://b"))),
            NodeDefinition.Join(id = "fan-in", next = "after"),
            NodeDefinition.Task("after", NodeActionDef(TaskMode.SYNC, httpCall("http://after"))),
        ),
    )

    /** a → switch → (hit | miss), so the trace shows which branch a condition picked. */
    private fun switchDefinition(condition: String) = SagaDefinitionV2(
        name = "switch-parity",
        version = "1",
        failureHandling = FailureHandling.Retry(1, 0),
        entrypoint = "a",
        nodes = listOf(
            NodeDefinition.Task("a", NodeActionDef(TaskMode.SYNC, httpCall("http://a")), next = "route"),
            NodeDefinition.Switch(
                id = "route",
                cases = listOf(run.trama.saga.SwitchCaseDef("hit", kotlinx.serialization.json.Json.parseToJsonElement(condition), "hit")),
                default = "miss",
            ),
            NodeDefinition.Task("hit", NodeActionDef(TaskMode.SYNC, httpCall("http://hit"))),
            NodeDefinition.Task("miss", NodeActionDef(TaskMode.SYNC, httpCall("http://miss"))),
        ),
    )

    @Test
    fun `switch conditions see the same names in the dry-run as in production`() {
        val conditions = listOf(
            """{"==":[{"var":"payload.method"},"pix"]}""",
            """{"==":[{"var":"input.method"},"pix"]}""",
            """{"==":[{"var":"prev.body.status"},"ok"]}""",
            """{"==":[{"var":"nodes.a.response.body.status"},"ok"]}""",
            """{"==":[{"var":"step.a.body.status"},"ok"]}""",
        )
        val payload = mapOf("method" to JsonPrimitive("pix"))
        val aBody = JsonObject(mapOf("status" to JsonPrimitive("ok")))

        for (condition in conditions) {
            val scenario = DryRunScenario(
                payload = JsonObject(payload),
                steps = mapOf(
                    "a" to StepMock(status = 200, body = aBody),
                    "hit" to StepMock(status = 200, body = JsonObject(emptyMap())),
                    "miss" to StepMock(status = 200, body = JsonObject(emptyMap())),
                ),
            )
            val simulated = DryRunSimulator().run(switchDefinition(condition), scenario)
                .entries.filterIsInstance<TraceEntry.Switch>().single()

            val runtime = run.trama.saga.workflow.SwitchNodeHandler.evaluate(
                run.trama.saga.workflow.DefinitionNormalizer.normalize(switchDefinition(condition)).nodes.getValue("route")
                    as run.trama.saga.workflow.SwitchNode,
                execution = run.trama.saga.SagaExecution(
                    definition = run.trama.saga.SagaDefinition("switch-parity", "1", FailureHandling.Retry(1, 0), steps = emptyList()),
                    id = java.util.UUID.randomUUID(),
                    startedAt = java.time.Instant.now(),
                    currentStepIndex = 0,
                    state = run.trama.saga.ExecutionState.InProgress(activeNodeId = "route"),
                ),
                payload = payload.mapValues { run.trama.saga.PayloadValue(it.value) },
                stepResults = listOf(run.trama.saga.StepResult(0, "a", aBody, null)),
            )

            assertEquals("hit", runtime.targetNodeId, "production must match: $condition")
            assertEquals(runtime.targetNodeId, simulated.targetNodeId, "dry-run must agree with production: $condition")
        }
    }

    @Test
    fun `simulates split branches and continues after join`() {
        val scenario = DryRunScenario(
            steps = mapOf(
                "branch-a" to StepMock(status = 200, body = JsonObject(mapOf("ok" to JsonPrimitive(true)))),
                "branch-b" to StepMock(status = 200, body = JsonObject(mapOf("ok" to JsonPrimitive(true)))),
                "after" to StepMock(status = 200, body = JsonObject(mapOf("done" to JsonPrimitive(true)))),
            ),
        )

        val result = DryRunSimulator().run(splitJoinDefinition(), scenario)

        assertEquals(SimOutcome.SUCCEEDED, result.outcome)
        val split = result.entries.filterIsInstance<TraceEntry.Split>().singleOrNull()
            ?: error("expected a Split trace entry, got: ${result.entries}")
        assertEquals(setOf("branch-a", "branch-b"), split.branches.keys)
        assertTrue(
            split.branches.getValue("branch-a").any { it is TraceEntry.Task && it.nodeId == "branch-a" },
            "expected branch-a's own trace to contain its task",
        )
        assertTrue(
            result.entries.any { it is TraceEntry.Join && it.nodeId == "fan-in" },
            "expected a Join trace entry after the branches",
        )
        assertTrue(
            result.entries.any { it is TraceEntry.Task && it.nodeId == "after" },
            "expected trace to continue after the join",
        )
    }

    @Test
    fun `split branch failure marks the whole simulation failed`() {
        val scenario = DryRunScenario(
            steps = mapOf(
                "branch-a" to StepMock(status = 200, body = JsonObject(emptyMap())),
                "branch-b" to StepMock(status = 500, body = JsonObject(emptyMap())),
            ),
        )

        val result = DryRunSimulator().run(splitJoinDefinition(), scenario)

        assertEquals(SimOutcome.FAILED, result.outcome)
        assertEquals("branch-b", result.failureNodeId)
        assertTrue(
            result.entries.none { it is TraceEntry.Task && it.nodeId == "after" },
            "must not continue past the join when a branch failed",
        )
    }

    @Test
    fun `nested split inside a branch is simulated recursively`() {
        val def = SagaDefinitionV2(
            name = "nested-split",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "outer-split",
            nodes = listOf(
                NodeDefinition.Split(id = "outer-split", branches = listOf("inner-split", "branch-b"), join = "outer-join"),
                NodeDefinition.Split(id = "inner-split", branches = listOf("branch-a1", "branch-a2"), join = "inner-join"),
                NodeDefinition.Task("branch-a1", NodeActionDef(TaskMode.SYNC, httpCall("http://a1"))),
                NodeDefinition.Task("branch-a2", NodeActionDef(TaskMode.SYNC, httpCall("http://a2"))),
                NodeDefinition.Join(id = "inner-join"),
                NodeDefinition.Task("branch-b", NodeActionDef(TaskMode.SYNC, httpCall("http://b"))),
                NodeDefinition.Join(id = "outer-join"),
            ),
        )
        val scenario = DryRunScenario(
            steps = mapOf(
                "branch-a1" to StepMock(status = 200, body = JsonObject(emptyMap())),
                "branch-a2" to StepMock(status = 200, body = JsonObject(emptyMap())),
                "branch-b" to StepMock(status = 200, body = JsonObject(emptyMap())),
            ),
        )

        val result = DryRunSimulator().run(def, scenario)

        assertEquals(SimOutcome.SUCCEEDED, result.outcome)
        val outerSplit = result.entries.filterIsInstance<TraceEntry.Split>().single { it.nodeId == "outer-split" }
        val innerBranchTrace = outerSplit.branches.getValue("inner-split")
        assertTrue(
            innerBranchTrace.any { it is TraceEntry.Split && it.nodeId == "inner-split" },
            "expected the nested split to appear inside the outer branch's own trace",
        )
    }
}
