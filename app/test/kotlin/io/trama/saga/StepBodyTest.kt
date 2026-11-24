package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

class StepBodyTest {
    @Test
    fun `empty or blank bodies have no value`() {
        assertNull(StepBody.value(null))
        assertNull(StepBody.value(""))
        assertNull(StepBody.value("  \n"))
        assertNull(StepBody.storedJson(""))
    }

    @Test
    fun `valid JSON is kept, any other text becomes a JSON string`() {
        assertEquals(Json.parseToJsonElement("""{"a":[1,"x"]}"""), StepBody.value("""{"a":[1,"x"]}"""))
        assertEquals(JsonNull, StepBody.value("null"))
        assertEquals(JsonPrimitive(""), StepBody.value("\"\""))
        assertEquals(JsonPrimitive("ok"), StepBody.value("ok"))
        assertEquals(JsonPrimitive("<html>oops</html>"), StepBody.value("<html>oops</html>"))
    }

    @Test
    fun `the stored JSON reads back as the same value`() {
        listOf("""{"b":2,"a":1}""", "ok", "plain text", "\"quoted\"", "12", "true", "null").forEach { raw ->
            assertEquals(StepBody.value(raw), StepBody.value(StepBody.storedJson(raw)), raw)
        }
    }
}
