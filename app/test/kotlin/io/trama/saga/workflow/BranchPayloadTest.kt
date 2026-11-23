package run.trama.saga.workflow

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.PayloadValue
import run.trama.saga.RESERVED_NODES_KEY
import run.trama.saga.StepResult
import run.trama.saga.TaskMode
import run.trama.saga.TemplateEscaping
import run.trama.saga.TemplateString

class BranchPayloadTest {

    private fun call(url: String, body: String? = null) =
        HttpCall(TemplateString(url), if (body == null) HttpVerb.GET else HttpVerb.POST, body = body?.let(::TemplateString))

    private fun task(id: String, request: HttpCall, next: String? = null, compensation: HttpCall? = null, callback: CallbackConfig? = null) =
        TaskNode(id, TaskAction(if (callback == null) TaskMode.SYNC else TaskMode.ASYNC, request, callbackConfig = callback), compensation, next)

    private fun logic(raw: String): JsonElement = Json.parseToJsonElement(raw)

    /** pre → s(branches) → j; [branchNodes] are the nodes inside the split. */
    private fun workflow(vararg branchNodes: WorkflowNode, branches: List<String> = listOf(branchNodes.first().id), onSuccess: HttpCall? = null) =
        WorkflowDefinition(
            name = "refs",
            version = "1",
            failureHandling = FailureHandling.Retry(0, 0),
            entrypoint = "pre",
            nodes = (
                listOf(task("pre", call("http://pre"), next = "s"), SplitNode("s", branches, "j"), JoinNode("j", next = "after"),
                    task("after", call("http://after/{{payload._nodes.late.x}}"))) + branchNodes
                ).associateBy { it.id },
            onSuccessCallback = onSuccess,
        )

    private fun selection(vararg branchNodes: WorkflowNode, onSuccess: HttpCall? = null) =
        workflow(*branchNodes, onSuccess = onSuccess).branchNodeSelection("s")

    private fun names(vararg n: String) = BranchNodeSelection(all = false, names = n.toSet())

    @Test
    fun `template references are collected per node, from every templated field`() {
        val sel = selection(
            task(
                "b",
                call("http://b/{{payload._nodes.a.x}}", body = """{"v":"{{payload._nodes.c.deep.field}}","w":"{{input._nodes.d}}"}""")
                    .copy(headers = mapOf("X-E" to TemplateString("{{ payload._nodes.e-1.y }}"))),
                compensation = call("http://undo/{{payload._nodes.f.z}}"),
                next = "b2",
            ),
            task("b2", call("http://b2/{{payload._nodes.g.list[0]}}")),
        )
        assertEquals(names("a", "c", "d", "e-1", "f", "g"), sel)
    }

    @Test
    fun `nodes after the join are not part of the split's subgraph`() {
        assertEquals(names(), selection(task("b", call("http://b"))))
    }

    @Test
    fun `json-logic var paths in switches and callback conditions are collected`() {
        val sel = selection(
            SwitchNode("b", listOf(SwitchCase("c", logic("""{"==":[{"var":"payload._nodes.a.k"},1]}"""), "c")), defaultTarget = "c"),
            task(
                "c",
                call("http://c"),
                callback = CallbackConfig(1000, successWhen = logic("""{"var":["input._nodes.b2.ok", false]}"""), failureWhen = logic("""{"missing":["payload._nodes.m"]}""")),
            ),
        )
        assertEquals(names("a", "b2", "m"), sel)
    }

    @Test
    fun `workflow callbacks are scanned, since every branch fires them`() {
        assertEquals(names("cb"), selection(task("b", call("http://b")), onSuccess = call("http://done/{{payload._nodes.cb.id}}")))
    }

    @Test
    fun `references that cannot be resolved statically carry every node`() {
        val dynamic = listOf(
            task("b", call("http://b/{{payload._nodes}}")),
            task("b", call("http://b", body = """{"all":"{{#payload._nodes}}{{x}}{{/payload._nodes}}"}""")),
            task("b", call("http://b", body = """{"all":"{{#payload}}{{_nodes.a}}{{/payload}}"}""")),
            task("b", call("http://b", body = """{"all":"{{payload}}"}""")),
            task("b", call("http://b/{{.}}")),
            SwitchNode("b", listOf(SwitchCase(null, logic("""{"var":{"cat":["payload._nodes.",{"var":"payload.which"}]}}"""), "x")), "x"),
            SwitchNode("b", listOf(SwitchCase(null, logic("""{"!!":{"var":"payload"}}"""), "x")), "x"),
            SwitchNode("b", listOf(SwitchCase(null, logic("""{"!!":{"var":""}}"""), "x")), "x"),
            SwitchNode("b", listOf(SwitchCase(null, logic("""{"!!":{"var":"payload._nodes"}}"""), "x")), "x"),
        )
        dynamic.forEach { node -> assertEquals(BranchNodeSelection.ALL, selection(node), "expected ALL for $node") }
    }

    @Test
    fun `a nested split propagates its branches' references to the outer split`() {
        val wf = workflow(
            task("b", call("http://b"), next = "inner"),
            SplitNode("inner", listOf("leaf"), "inner-j"),
            task("leaf", call("http://leaf/{{payload._nodes.pre.x}}/{{payload._nodes.b.y}}")),
            JoinNode("inner-j", next = "tail"),
            task("tail", call("http://tail/{{payload._nodes.t.z}}")),
        )
        assertEquals(names("pre", "b", "t"), wf.branchNodeSelection("s"))
        assertEquals(names("pre", "b"), wf.branchNodeSelection("inner"))
    }

    @Test
    fun `build merges inherited _nodes with the parent's own results, the parent winning, and drops split traces`() {
        val wf = workflow(task("b", call("http://b/{{payload._nodes}}")))
        val inherited = JsonObject(mapOf("pre" to JsonPrimitive("old"), "grand" to JsonPrimitive("g")))
        val payload = BranchPayload.build(
            parentPayload = mapOf("k" to PayloadValue(JsonPrimitive(1)), RESERVED_NODES_KEY to PayloadValue(inherited)),
            stepResults = listOf(
                StepResult(0, "pre", logic("""{"x":1}"""), null),
                StepResult(1, "s", null, null), // a split's fan-out record
                StepResult(1, "j", logic("""{"allSucceeded":true}"""), null),
                StepResult(2, "undone", null, logic("""{"down":1}""")),
            ),
            workflow = wf,
            split = wf.nodes.getValue("s") as SplitNode,
        )
        assertEquals(JsonPrimitive(1), payload.getValue("k").value)
        assertEquals(
            JsonObject(
                mapOf(
                    "grand" to JsonPrimitive("g"),
                    "pre" to logic("""{"x":1}"""),
                    "j" to logic("""{"allSucceeded":true}"""),
                    "undone" to logic("""{"down":1}"""),
                ),
            ),
            payload.getValue(RESERVED_NODES_KEY).value,
        )
    }

    @Test
    fun `build keeps only the referenced nodes`() {
        val wf = workflow(task("b", call("http://b/{{payload._nodes.pre.x}}")))
        val payload = BranchPayload.build(
            emptyMap(),
            listOf(StepResult(0, "pre", logic("""{"x":1}"""), null), StepResult(1, "other", logic("{}"), null)),
            wf,
            wf.nodes.getValue("s") as SplitNode,
        )
        assertEquals(JsonObject(mapOf("pre" to logic("""{"x":1}"""))), payload.getValue(RESERVED_NODES_KEY).value)
    }

    @Test
    fun `mustache and json-logic resolve underscore-prefixed paths`() {
        val nodes = JsonObject(mapOf("load" to logic("""{"customer":"c-9","n":3}""")))
        val payload = mapOf(RESERVED_NODES_KEY to PayloadValue(nodes))
        val ctx = SwitchNodeHandler.evaluationContext(payload, emptyList())
        assertTrue(JsonLogicEvaluator.evaluateBool(logic("""{"==":[{"var":"payload._nodes.load.customer"},"c-9"]}"""), ctx))
        assertTrue(JsonLogicEvaluator.evaluateBool(logic("""{"==":[{"var":"input._nodes.load.n"},3]}"""), ctx))
        val rendered = MustacheTemplateRenderer().render(
            TemplateString("{{payload._nodes.load.customer}}/{{input._nodes.load.n}}"),
            mapOf("payload" to mapOf(RESERVED_NODES_KEY to mapOf("load" to mapOf("customer" to "c-9", "n" to 3)))).let {
                it + ("input" to it["payload"])
            },
            TemplateEscaping.NONE,
        )
        assertEquals("c-9/3", rendered)
    }
}
