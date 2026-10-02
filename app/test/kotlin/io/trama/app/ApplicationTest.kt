package run.trama.app

import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import java.util.UUID
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import run.trama.e2e.E2EContainers
import run.trama.saga.store.IntegrationDb
import run.trama.saga.store.SagaRepository

/**
 * The API-only profile (runtime.enabled=false): no workers, but runs are accepted, persisted and
 * queued for worker processes instead of being refused.
 */
class ApplicationTest {
    private val props = mutableListOf<String>()

    private fun set(key: String, value: String) {
        props += key
        System.setProperty(key, value)
    }

    private fun apiOnlyAgainstTestContainers() {
        IntegrationDb.assumeDocker()
        val pg = E2EContainers.postgres
        val redis = E2EContainers.redis
        set("runtime.enabled", "false")
        set("database.host", pg.host)
        set("database.port", pg.firstMappedPort.toString())
        set("database.database", pg.databaseName)
        set("database.user", pg.username)
        set("database.password", pg.password)
        set("redis.url", "redis://${redis.host}:${redis.getMappedPort(6379)}")
    }

    @AfterTest
    fun clearProperties() {
        props.forEach { System.clearProperty(it) }
    }

    @Test
    fun healthz() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            assertEquals(200, client.get("/healthz").status.value)
            assertEquals(200, client.get("/readyz").status.value)
        }
    }

    @Test
    fun `an API-only process accepts and persists runs for the workers`() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            val response = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody(
                    """
                    {"definition": {"name": "api-only-${UUID.randomUUID()}", "version": "1",
                      "failureHandling": {"type": "retry", "maxAttempts": 0, "delayMillis": 0},
                      "entrypoint": "t1",
                      "nodes": [{"kind": "task", "id": "t1",
                        "action": {"mode": "sync", "request": {"url": "http://localhost:1/x", "verb": "POST"}}}]},
                     "payload": {}}
                    """.trimIndent()
                )
            }
            assertEquals(200, response.status.value, response.bodyAsText())
            val id = Json.parseToJsonElement(response.bodyAsText()).jsonObject.getValue("id").jsonPrimitive.content
            val status = runBlocking { SagaRepository(IntegrationDb.client).getExecutionStatus(UUID.fromString(id)) }
            assertEquals("IN_PROGRESS", assertNotNull(status, "the run's row is written at admission").status)
        }
    }
}
