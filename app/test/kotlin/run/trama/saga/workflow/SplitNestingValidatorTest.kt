package run.trama.saga.workflow

import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SwitchCaseDef
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The static half of split.maxDepth: definitions whose splits nest too deep are refused. */
class SplitNestingValidatorTest {

    private fun task(id: String, next: String? = null) = NodeDefinition.Task(
        id,
        NodeActionDef(TaskMode.SYNC, HttpCall(TemplateString("http://svc/$id"), HttpVerb.POST)),
        next = next,
    )

    private fun definition(entrypoint: String, nodes: List<NodeDefinition>) = SagaDefinitionV2(
        name = "nesting",
        version = "1",
        failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
        entrypoint = entrypoint,
        nodes = nodes,
    )

    /** Splits nested [levels] deep: branch `deeper<k>` of split `s<k>` runs split `s<k+1>`. */
    private fun nested(levels: Int): SagaDefinitionV2 = definition(
        "s1",
        (1..levels).flatMap { k ->
            listOf(
                NodeDefinition.Split("s$k", branches = listOf("deeper$k", "leaf$k"), join = "j$k"),
                task("deeper$k", next = if (k < levels) "s${k + 1}" else null),
                task("leaf$k"),
                NodeDefinition.Join("j$k"),
            )
        },
    )

    private fun validate(def: SagaDefinitionV2, max: Int = 3) = WorkflowDefinitionValidator.validate(def, max)

    @Test
    fun `nesting up to split maxDepth is accepted, deeper is refused`() {
        assertTrue(WorkflowDefinitionValidator.validate(nested(3)).isEmpty(), "the default maxDepth is 3")
        assertEquals(listOf("split nesting depth 4 exceeds split.maxDepth 3"), validate(nested(4)))
        assertEquals(listOf("split nesting depth 5 exceeds split.maxDepth 3"), validate(nested(5)))
        assertEquals(listOf("split nesting depth 2 exceeds split.maxDepth 1"), validate(nested(2), 1))
        assertEquals(listOf("split nesting depth 1 exceeds split.maxDepth 0"), validate(nested(1), 0))
        assertTrue(validate(definition("a", listOf(task("a", "b"), task("b"))), 0).isEmpty(), "no split: depth 0")
    }

    @Test
    fun `splits one after another, or after a nested join, do not nest`() {
        val sequential = definition(
            "s1",
            listOf(
                NodeDefinition.Split("s1", listOf("a1", "b1"), "j1"),
                task("a1"), task("b1"),
                NodeDefinition.Join("j1", next = "s2"),
                NodeDefinition.Split("s2", listOf("a2", "b2"), "j2"),
                task("a2"), task("b2"),
                NodeDefinition.Join("j2"),
            ),
        )
        assertTrue(validate(sequential, 1).isEmpty())

        // Inside a branch: a nested split, its join, then another nested split. Still 2 levels.
        val branchSequence = definition(
            "outer",
            listOf(
                NodeDefinition.Split("outer", listOf("in1", "plain"), "outer-join"),
                NodeDefinition.Split("in1", listOf("x1", "y1"), "in1-join"),
                task("x1"), task("y1"),
                NodeDefinition.Join("in1-join", next = "in2"),
                NodeDefinition.Split("in2", listOf("x2", "y2"), "in2-join"),
                task("x2"), task("y2"),
                NodeDefinition.Join("in2-join"),
                task("plain"),
                NodeDefinition.Join("outer-join"),
            ),
        )
        assertTrue(validate(branchSequence, 2).isEmpty())
        assertEquals(listOf("split nesting depth 2 exceeds split.maxDepth 1"), validate(branchSequence, 1))
    }

    @Test
    fun `every switch path counts`() {
        val def = definition(
            "outer",
            listOf(
                NodeDefinition.Split("outer", listOf("route", "plain"), "outer-join"),
                NodeDefinition.Switch(
                    "route",
                    cases = listOf(SwitchCaseDef(name = "deep", whenExpression = JsonPrimitive(false), target = "inner")),
                    default = "plain2",
                ),
                NodeDefinition.Split("inner", listOf("x", "y"), "inner-join"),
                task("x"), task("y"), task("plain"), task("plain2"),
                NodeDefinition.Join("inner-join"),
                NodeDefinition.Join("outer-join"),
            ),
        )
        assertEquals(listOf("split nesting depth 2 exceeds split.maxDepth 1"), validate(def, 1))
    }

    @Test
    fun `a branch that reaches its own split again is refused, a loop around a split is not`() {
        val recursive = definition(
            "fan",
            listOf(
                NodeDefinition.Split("fan", listOf("again", "leaf"), "fan-join"),
                task("again", next = "fan"),
                task("leaf"),
                NodeDefinition.Join("fan-join"),
            ),
        )
        assertEquals(listOf("a split is reachable from its own branches, so its nesting is unbounded"), validate(recursive))

        val loop = definition(
            "s",
            listOf(
                NodeDefinition.Split("s", listOf("a", "b"), "j"),
                task("a"), task("b"),
                NodeDefinition.Join("j", next = "again"),
                task("again", next = "s"),
            ),
        )
        assertTrue(validate(loop, 1).isEmpty(), validate(loop, 1).toString())
    }
}
