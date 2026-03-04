package io.trama.saga

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class TemplateStringSerializerTest {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }

    @Test
    fun `TemplateString accepts direct string`() {
        val decoded = json.decodeFromString<TemplateString>("\"{{payload.initial_value}}\"")
        assertEquals("{{payload.initial_value}}", decoded.value)
    }

    @Test
    fun `TemplateString accepts wrapped object`() {
        val decoded = json.decodeFromString<TemplateString>("""{"value":"{{step.0.up.body.result}}"}""")
        assertEquals("{{step.0.up.body.result}}", decoded.value)
    }

    @Test
    fun `TemplateString accepts raw object and stores it as json string`() {
        val decoded = json.decodeFromString<TemplateString>("""{"step":"square-1","lastKnown":"payload.initial_value"}""")
        assertEquals("""{"step":"square-1","lastKnown":"payload.initial_value"}""", decoded.value)
    }

    @Test
    fun `TemplateString encoding remains wrapped with value`() {
        val encoded = json.encodeToString(TemplateString("http://127.0.0.1:5002/square"))
        assertEquals("""{"value":"http://127.0.0.1:5002/square"}""", encoded)
    }
}
