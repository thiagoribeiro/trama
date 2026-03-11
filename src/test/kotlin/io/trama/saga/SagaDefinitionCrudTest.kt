package run.trama.saga

import io.mockk.every
import io.mockk.mockk
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.SagaRepository
import java.util.UUID
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SagaDefinitionCrudTest {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }

    @Test
    fun `getDefinition reads from cache when available`() {
        val db = mockk<DatabaseClient>(relaxed = true)
        every { db.withConnection<Int>(any()) } returns 1
        val repo = SagaRepository(db)
        val id = UUID.randomUUID()
        repo.insertDefinition(id, "s1", "1", "{}")
        val result = repo.getDefinition(id)
        assertNotNull(result)
        assertEquals("s1", result.name)
    }

    @Test
    fun `getDefinitionByNameVersion reads from cache when available`() {
        val db = mockk<DatabaseClient>(relaxed = true)
        var calls = 0
        every { db.withConnection<Int>(any()) } answers {
            calls += 1
            1
        }
        val repo = SagaRepository(db)
        val id = UUID.randomUUID()

        repo.insertDefinition(id, "square-chain-demo", "v1", "{}")
        val first = repo.getDefinitionByNameVersion("square-chain-demo", "v1")
        val second = repo.getDefinitionByNameVersion("square-chain-demo", "v1")

        assertNotNull(first)
        assertNotNull(second)
        assertEquals(id, first.id)
        assertEquals(id, second.id)
        assertEquals(1, calls)
    }

    @Test
    fun `insertDefinition saves square chain definition with template placeholders`() {
        val db = mockk<DatabaseClient>(relaxed = true)
        every { db.withConnection<Int>(any()) } returns 1
        val repo = SagaRepository(db)
        val id = UUID.randomUUID()

        val definition = SagaDefinition(
            name = "square-chain-demo",
            version = "v1772484306",
            failureHandling = FailureHandling.Retry(maxAttempts = 1, delayMillis = 200),
            steps = listOf(
                squareStep("square-1", """{{payload.initial_value}}"""),
                squareStep("square-2", """{{step.0.up.body.result}}"""),
                squareStep("square-3", """{{step.1.up.body.result}}"""),
                squareStep("square-4", """{{step.2.up.body.result}}"""),
            ),
        )

        val definitionJson = json.encodeToString(SagaDefinition.serializer(), definition)
        val inserted = repo.insertDefinition(id, definition.name, definition.version, definitionJson)

        assertEquals(true, inserted)
        val stored = repo.getDefinition(id)
        assertNotNull(stored)
        assertEquals("square-chain-demo", stored.name)
        assertEquals("v1772484306", stored.version)
        assertEquals(definitionJson, stored.definitionJson)

        val decoded = json.decodeFromString(SagaDefinition.serializer(), stored.definitionJson)
        assertEquals(definition.name, decoded.name)
        assertEquals(definition.version, decoded.version)
        assertEquals(4, decoded.steps.size)
        assertEquals(
            """{"value": {{step.2.up.body.result}}}""",
            decoded.steps[3].up.body?.value,
        )
    }

    @Test
    fun `insertDefinition saves definition decoded from direct-content json`() {
        val db = mockk<DatabaseClient>(relaxed = true)
        every { db.withConnection<Int>(any()) } returns 1
        val repo = SagaRepository(db)
        val id = UUID.randomUUID()

        val rawJson =
            """
            {
                "name": "square-chain-demo",
                "version": "v1772579173",
                "failureHandling": {
                    "type": "retry",
                    "maxAttempts": 1,
                    "delayMillis": 200
                },
                "steps": [
                    {
                        "name": "square-1",
                        "up": {
                            "url": "http://127.0.0.1:5002/square",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": "payload.initial_value"
                        },
                        "down": {
                            "url": "http://127.0.0.1:5002/compensate",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": {
                                "step": "square-{index + 1}",
                                "lastKnown": "payload.initial_value"
                            }
                        }
                    },
                    {
                        "name": "square-2",
                        "up": {
                            "url": "http://127.0.0.1:5002/square",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": "step.0.up.body.result"
                        },
                        "down": {
                            "url": "http://127.0.0.1:5002/compensate",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": {
                                "step": "square-{index + 1}",
                                "lastKnown": "step.0.up.body.result"
                            }
                        }
                    },
                    {
                        "name": "square-3",
                        "up": {
                            "url": "http://127.0.0.1:5002/square",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": "step.1.up.body.result"
                        },
                        "down": {
                            "url": "http://127.0.0.1:5002/compensate",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": {
                                "step": "square-{index + 1}",
                                "lastKnown": "step.1.up.body.result"
                            }
                        }
                    },
                    {
                        "name": "square-4",
                        "up": {
                            "url": "http://127.0.0.1:5002/square",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": "step.2.up.body.result"
                        },
                        "down": {
                            "url": "http://127.0.0.1:5002/compensate",
                            "verb": "POST",
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "body": {
                                "step": "square-{index + 1}",
                                "lastKnown": "step.2.up.body.result"
                            }
                        }
                    }
                ]
            }
            """.trimIndent()

        val req = json.decodeFromString<SagaDefinitionCreateRequest>(rawJson)
        val definition = SagaDefinition(
            name = req.name,
            version = req.version,
            failureHandling = req.failureHandling,
            steps = req.steps,
            onSuccessCallback = req.onSuccessCallback,
            onFailureCallback = req.onFailureCallback,
        )
        val definitionJson = json.encodeToString(SagaDefinition.serializer(), definition)
        val inserted = repo.insertDefinition(id, definition.name, definition.version, definitionJson)

        assertEquals(true, inserted)
        val stored = repo.getDefinition(id)
        assertNotNull(stored)
        assertEquals("square-chain-demo", stored.name)
        assertEquals("v1772579173", stored.version)
        assertEquals("payload.initial_value", definition.steps[0].up.body?.value)
        assertEquals(
            """{"step":"square-{index + 1}","lastKnown":"payload.initial_value"}""",
            definition.steps[0].down.body?.value,
        )
    }

    private fun squareStep(name: String, template: String): SagaStep {
        return SagaStep(
            name = name,
            up = HttpCall(
                url = TemplateString("http://127.0.0.1:5002/square"),
                verb = HttpVerb.POST,
                headers = mapOf("Content-Type" to TemplateString("application/json")),
                body = TemplateString("""{"value": $template}"""),
            ),
            down = HttpCall(
                url = TemplateString("http://127.0.0.1:5002/compensate"),
                verb = HttpVerb.POST,
                headers = mapOf("Content-Type" to TemplateString("application/json")),
                body = TemplateString("""{"step": "$name", "lastKnown": $template}"""),
            ),
        )
    }
}
