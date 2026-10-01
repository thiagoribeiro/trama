package run.trama.saga.workflow

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

class JsonLogicEvaluatorTest {

    private fun rule(raw: String): JsonElement = Json.parseToJsonElement(raw)

    @Test
    fun `boolean result is returned as-is`() {
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"==":[1,1]}"""), emptyMap()))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"==":[1,2]}"""), emptyMap()))
    }

    @Test
    fun `nested var lookup reads from maps`() {
        val data = mapOf("payload" to mapOf("order" to mapOf("amount" to 150)))
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{">":[{"var":"payload.order.amount"},100]}"""), data))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{">":[{"var":"payload.order.amount"},200]}"""), data))
    }

    @Test
    fun `numeric result is truthy when non-zero`() {
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"+":[1,2]}"""), emptyMap()))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"-":[2,2]}"""), emptyMap()))
    }

    @Test
    fun `string result is truthy when non-empty`() {
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"var":"s"}"""), mapOf("s" to "x")))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"var":"s"}"""), mapOf("s" to "")))
    }

    @Test
    fun `missing variable evaluates to false`() {
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"var":"does.not.exist"}"""), emptyMap()))
    }

    @Test
    fun `logical operators compose`() {
        val data = mapOf("a" to true, "b" to false)
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"or":[{"var":"a"},{"var":"b"}]}"""), data))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"and":[{"var":"a"},{"var":"b"}]}"""), data))
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"!":{"var":"b"}}"""), data))
    }

    @Test
    fun `in operator matches list membership`() {
        val data = mapOf("status" to "APPROVED")
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("""{"in":[{"var":"status"},["APPROVED","SETTLED"]]}"""), data))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"in":[{"var":"status"},["DECLINED"]]}"""), data))
    }

    @Test
    fun `unknown operator returns false instead of throwing`() {
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("""{"no_such_op":[1,2]}"""), emptyMap()))
    }

    @Test
    fun `literal true and false rules`() {
        assertTrue(JsonLogicEvaluator.evaluateBool(rule("true"), emptyMap()))
        assertFalse(JsonLogicEvaluator.evaluateBool(rule("false"), emptyMap()))
    }
}
