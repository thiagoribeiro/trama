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
