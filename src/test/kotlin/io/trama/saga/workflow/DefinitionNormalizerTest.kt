package run.trama.saga.workflow

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import run.trama.saga.CallbackConfigDef
import run.trama.saga.FailureHandling
import run.trama.saga.HttpCall
import run.trama.saga.HttpVerb
import run.trama.saga.NodeDefinition
import run.trama.saga.NodeActionDef
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaStep
import run.trama.saga.SwitchCaseDef
import run.trama.saga.TaskMode
import run.trama.saga.TemplateString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertNotNull

class DefinitionNormalizerTest {

    // ── v1 (steps) normalization ──────────────────────────────────────────────

    @Test
    fun `v1 single step produces one terminal TaskNode`() {
        val def = v1Definition(
            SagaStep("pay", up = httpCall("http://pay"), down = httpCall("http://pay/cancel")),
        )

        val workflow = DefinitionNormalizer.normalize(def)

        assertEquals("pay", workflow.entrypoint)
        assertEquals(1, workflow.nodes.size)

        val node = workflow.nodes["pay"]
        assertIs<TaskNode>(node)
        assertEquals(TaskMode.SYNC, node.action.mode)
        assertEquals("http://pay", node.action.request.url.value)
        assertEquals("http://pay/cancel", node.compensation!!.url.value)
        assertNull(node.next, "single step should be terminal")
    }

    @Test
    fun `v1 three steps are chained in order`() {
        val def = v1Definition(
            SagaStep("a", httpCall("http://a"), httpCall("http://a/down")),
            SagaStep("b", httpCall("http://b"), httpCall("http://b/down")),
            SagaStep("c", httpCall("http://c"), httpCall("http://c/down")),
        )

        val workflow = DefinitionNormalizer.normalize(def)

        assertEquals("a", workflow.entrypoint)
        assertEquals(3, workflow.nodes.size)

        val a = workflow.nodes["a"] as TaskNode
        val b = workflow.nodes["b"] as TaskNode
        val c = workflow.nodes["c"] as TaskNode

        assertEquals("b", a.next)
        assertEquals("c", b.next)
        assertNull(c.next, "last step must be terminal")
    }

    @Test
    fun `v1 step up maps to action and down maps to compensation`() {
        val up = HttpCall(TemplateString("http://svc/do"), HttpVerb.POST)
        val down = HttpCall(TemplateString("http://svc/undo"), HttpVerb.DELETE)
        val def = v1Definition(SagaStep("step", up, down))

        val workflow = DefinitionNormalizer.normalize(def)
        val node = workflow.nodes["step"] as TaskNode

        assertEquals(up.url.value, node.action.request.url.value)
        assertEquals(up.verb, node.action.request.verb)
        assertEquals(down.url.value, node.compensation!!.url.value)
        assertEquals(down.verb, node.compensation.verb)
    }

    @Test
    fun `v1 metadata is preserved in IR`() {
        val def = v1Definition(SagaStep("s", httpCall("http://x"), httpCall("http://x")))
            .copy(name = "my-saga", version = "2", onSuccessCallback = httpCall("http://cb"))

        val workflow = DefinitionNormalizer.normalize(def)

        assertEquals("my-saga", workflow.name)
        assertEquals("2", workflow.version)
        assertNotNull(workflow.onSuccessCallback)
        assertEquals("http://cb", workflow.onSuccessCallback!!.url.value)
    }

    // ── v2 (nodes) normalization ──────────────────────────────────────────────

    @Test
    fun `v2 linear task nodes produce correct IR`() {
        val def = SagaDefinitionV2(
            name = "checkout",
            version = "2.0",
            failureHandling = FailureHandling.Retry(maxAttempts = 3, delayMillis = 500),
            entrypoint = "reserve",
            nodes = listOf(
                NodeDefinition.Task(
                    id = "reserve",
                    action = NodeActionDef(
                        mode = TaskMode.SYNC,
                        request = httpCall("http://inventory/reserve"),
                    ),
                    compensation = httpCall("http://inventory/release"),
                    next = "pay",
                ),
                NodeDefinition.Task(
                    id = "pay",
                    action = NodeActionDef(
                        mode = TaskMode.SYNC,
                        request = httpCall("http://payments/charge"),
                    ),
                    next = null,
                ),
            ),
        )

        val workflow = DefinitionNormalizer.normalize(def)

        assertEquals("checkout", workflow.name)
        assertEquals("reserve", workflow.entrypoint)
        assertEquals(2, workflow.nodes.size)

        val reserve = workflow.nodes["reserve"] as TaskNode
        assertEquals("pay", reserve.next)
        assertEquals("http://inventory/reserve", reserve.action.request.url.value)
        assertEquals("http://inventory/release", reserve.compensation!!.url.value)

        val pay = workflow.nodes["pay"] as TaskNode
        assertNull(pay.next)
    }

    @Test
    fun `v2 switch node maps to SwitchNode IR`() {
        val condition = JsonObject(mapOf("==" to JsonPrimitive("always")))
        val def = SagaDefinitionV2(
            name = "branched",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "choose",
            nodes = listOf(
                NodeDefinition.Switch(
                    id = "choose",
                    cases = listOf(
                        SwitchCaseDef(name = "pix", whenExpression = condition, target = "pix-node"),
                        SwitchCaseDef(name = "card", whenExpression = condition, target = "card-node"),
                    ),
                    default = "fallback",
                ),
                NodeDefinition.Task("pix-node", NodeActionDef(TaskMode.SYNC, httpCall("http://pix"))),
                NodeDefinition.Task("card-node", NodeActionDef(TaskMode.SYNC, httpCall("http://card"))),
                NodeDefinition.Task("fallback", NodeActionDef(TaskMode.SYNC, httpCall("http://fallback"))),
            ),
        )

        val workflow = DefinitionNormalizer.normalize(def)

        val choose = workflow.nodes["choose"]
        assertIs<SwitchNode>(choose)
        assertEquals(2, choose.cases.size)
        assertEquals("pix-node", choose.cases[0].target)
        assertEquals("card-node", choose.cases[1].target)
        assertEquals("fallback", choose.defaultTarget)
    }

    @Test
    fun `v2 action successStatusCodes override HttpCall defaults`() {
        val def = SagaDefinitionV2(
            name = "test",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "step",
            nodes = listOf(
                NodeDefinition.Task(
                    id = "step",
                    action = NodeActionDef(
                        mode = TaskMode.SYNC,
                        request = httpCall("http://svc"),
                        successStatusCodes = setOf(201),
                    ),
                ),
            ),
        )

        val workflow = DefinitionNormalizer.normalize(def)
        val node = workflow.nodes["step"] as TaskNode

        assertEquals(setOf(201), node.action.request.successStatusCodes)
    }

    @Test
    fun `v2 async task maps callbackConfig to IR`() {
        val def = SagaDefinitionV2(
            name = "async-flow",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "authorize",
            nodes = listOf(
                NodeDefinition.Task(
                    id = "authorize",
                    action = NodeActionDef(
                        mode = TaskMode.ASYNC,
                        request = httpCall("http://payments/authorize"),
                        acceptedStatusCodes = setOf(202),
                        callback = CallbackConfigDef(timeoutMillis = 60_000),
                    ),
                ),
            ),
        )

        val workflow = DefinitionNormalizer.normalize(def)
        val node = workflow.nodes["authorize"] as TaskNode

        assertEquals(TaskMode.ASYNC, node.action.mode)
        assertEquals(setOf(202), node.action.acceptedStatusCodes)
        assertNotNull(node.action.callbackConfig)
        assertEquals(60_000L, node.action.callbackConfig!!.timeoutMillis)
    }

    // ── v2 validation ─────────────────────────────────────────────────────────

    @Test
    fun `v2 valid definition passes validation`() {
        val def = SagaDefinitionV2(
            name = "valid",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "step",
            nodes = listOf(
                NodeDefinition.Task("step", NodeActionDef(TaskMode.SYNC, httpCall("http://svc"))),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(def)
        assertEquals(emptyList(), errors)
    }

    @Test
    fun `v2 unknown entrypoint produces error`() {
        val def = SagaDefinitionV2(
            name = "bad",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "missing",
            nodes = listOf(
                NodeDefinition.Task("step", NodeActionDef(TaskMode.SYNC, httpCall("http://svc"))),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(def)
        assert(errors.any { "entrypoint" in it && "missing" in it }) { "Expected entrypoint error, got: $errors" }
    }

    @Test
    fun `v2 switch case pointing to unknown node produces error`() {
        val condition = JsonObject(mapOf("==" to JsonPrimitive("x")))
        val def = SagaDefinitionV2(
            name = "bad",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "choose",
            nodes = listOf(
                NodeDefinition.Switch(
                    id = "choose",
                    cases = listOf(SwitchCaseDef(whenExpression = condition, target = "ghost")),
                    default = "choose",
                ),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(def)
        assert(errors.any { "ghost" in it }) { "Expected unknown target error, got: $errors" }
    }

    @Test
    fun `v2 async task without callbackConfig produces error`() {
        val def = SagaDefinitionV2(
            name = "bad",
            version = "1",
            failureHandling = FailureHandling.Retry(1, 0),
            entrypoint = "step",
            nodes = listOf(
                NodeDefinition.Task(
                    id = "step",
                    action = NodeActionDef(
                        mode = TaskMode.ASYNC,
                        request = httpCall("http://svc"),
                        // callback missing!
                    ),
                ),
            ),
        )

        val errors = WorkflowDefinitionValidator.validate(def)
        assert(errors.any { "callback" in it && "required" in it }) { "Expected callback error, got: $errors" }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun httpCall(url: String): HttpCall =
        HttpCall(TemplateString(url), HttpVerb.POST)

    // Name is derived from step names so each test gets a distinct cache key.
    private fun v1Definition(vararg steps: SagaStep): SagaDefinition =
        SagaDefinition(
            name = "test-${steps.joinToString("-") { it.name }}",
            version = "1",
            failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 100),
            steps = steps.toList(),
        )
}
