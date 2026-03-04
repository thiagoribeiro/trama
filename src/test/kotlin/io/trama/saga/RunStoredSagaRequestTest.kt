package io.trama.saga

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RunStoredSagaRequestTest {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }

    @Test
    fun `payload supports native json types`() {
        val raw =
            """
            {
              "payload": {
                "initial_value": 2.0,
                "enabled": true,
                "meta": {"source":"test"}
              }
            }
            """.trimIndent()

        val req = json.decodeFromString<RunStoredSagaRequest>(raw)
        assertEquals(JsonPrimitive(2.0), req.payload["initial_value"])
        assertEquals(JsonPrimitive(true), req.payload["enabled"])
        assertTrue(req.payload["meta"] is JsonObject)
    }
}
