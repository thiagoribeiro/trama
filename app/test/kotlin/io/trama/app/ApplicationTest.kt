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
import kotlin.test.assertTrue
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

    @Test
    fun `starting a saga whose payload carries the reserved _nodes key is rejected with 400`() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            val reserved = """{"_nodes": {"pre": {"x": 1}}, "orderId": "o-1"}"""
            val v2 = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody(
                    """
                    {"definition": {"name": "reserved-${UUID.randomUUID()}", "version": "1",
                      "failureHandling": {"type": "retry", "maxAttempts": 0, "delayMillis": 0},
                      "entrypoint": "t1",
                      "nodes": [{"kind": "task", "id": "t1",
                        "action": {"mode": "sync", "request": {"url": "http://localhost:1/x", "verb": "POST"}}}]},
                     "payload": $reserved}
                    """.trimIndent()
                )
            }
            assertEquals(400, v2.status.value, v2.bodyAsText())
            assertTrue("_nodes" in v2.bodyAsText(), v2.bodyAsText())

            val v1Definition = """
                {"name": "reserved-v1-${UUID.randomUUID()}", "version": "1",
                 "failureHandling": {"type": "retry", "maxAttempts": 0, "delayMillis": 0},
                 "steps": [{"name": "s1", "up": {"url": "http://localhost:1/x", "verb": "POST"}}]}
            """.trimIndent()
            val v1 = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody("""{"definition": $v1Definition, "payload": $reserved}""")
            }
            assertEquals(400, v1.status.value, v1.bodyAsText())

            val stored = client.post("/workflows/definitions/any-name/1/run") {
                contentType(ContentType.Application.Json)
                setBody("""{"payload": $reserved}""")
            }
            assertEquals(400, stored.status.value, stored.bodyAsText())
        }
    }

    private fun v2Run(name: String, nodes: String = """[{"kind": "task", "id": "t1",
            "action": {"mode": "sync", "request": {"url": "http://localhost:1/x", "verb": "POST"}}}]""", entrypoint: String = "t1") =
        """{"definition": ${v2Definition(name, nodes, entrypoint)}, "payload": {}}"""

    private fun v2Definition(name: String, nodes: String, entrypoint: String) = """
        {"name": "$name", "version": "1",
         "failureHandling": {"type": "retry", "maxAttempts": 0, "delayMillis": 0},
         "entrypoint": "$entrypoint", "nodes": $nodes}
    """.trimIndent()

    /** Splits nested [levels] deep: branch `deeper<k>` of split `s<k>` runs split `s<k+1>`. */
    private fun nestedSplitNodes(levels: Int): String = (1..levels).joinToString(",", "[", "]") { k ->
        val next = if (k < levels) ""","next": "s${k + 1}"""" else ""
        """
        {"kind": "split", "id": "s$k", "branches": ["deeper$k", "leaf$k"], "join": "j$k"},
        {"kind": "task", "id": "deeper$k", "action": {"mode": "sync", "request": {"url": "http://localhost:1/d", "verb": "POST"}}$next},
        {"kind": "task", "id": "leaf$k", "action": {"mode": "sync", "request": {"url": "http://localhost:1/l", "verb": "POST"}}},
        {"kind": "join", "id": "j$k"}
        """
    }

    @Test
    fun `a saga start begins at the depth in X-Trama-Split-Depth, 0 without it`() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            suspend fun startedDepth(header: String?): Int {
                val response = client.post("/workflows/run") {
                    contentType(ContentType.Application.Json)
                    header?.let { headers.append("X-Trama-Split-Depth", it) }
                    setBody(v2Run("depth-${UUID.randomUUID()}"))
                }
                assertEquals(200, response.status.value, response.bodyAsText())
                val id = UUID.fromString(Json.parseToJsonElement(response.bodyAsText()).jsonObject.getValue("id").jsonPrimitive.content)
                return assertNotNull(SagaRepository(IntegrationDb.client).loadCheckpoint(id)).splitDepth
            }
            assertEquals(0, startedDepth(null))
            assertEquals(2, startedDepth("2"))
            // Past the limit is still accepted: only a split it would run fails.
            assertEquals(5, startedDepth("5"))
        }
    }

    @Test
    fun `an invalid X-Trama-Split-Depth is rejected with 400`() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            listOf("abc", "-1", "1.5", "").forEach { value ->
                val inline = client.post("/workflows/run") {
                    contentType(ContentType.Application.Json)
                    headers.append("X-Trama-Split-Depth", value)
                    setBody(v2Run("bad-depth-${UUID.randomUUID()}"))
                }
                assertEquals(400, inline.status.value, "'$value': ${inline.bodyAsText()}")
                assertTrue("X-Trama-Split-Depth" in inline.bodyAsText(), inline.bodyAsText())

                val stored = client.post("/workflows/definitions/any-name/1/run") {
                    contentType(ContentType.Application.Json)
                    headers.append("X-Trama-Split-Depth", value)
                    setBody("""{"payload": {}}""")
                }
                assertEquals(400, stored.status.value, "'$value': ${stored.bodyAsText()}")
            }
        }
    }

    @Test
    fun `a definition nesting splits past split maxDepth is refused on register and on start`() {
        apiOnlyAgainstTestContainers()
        testApplication {
            application { module() }
            val tooDeep = v2Definition("too-deep-${UUID.randomUUID()}", nestedSplitNodes(4), "s1")
            val registered = client.post("/workflows/definitions") {
                contentType(ContentType.Application.Json)
                setBody(tooDeep)
            }
            assertEquals(400, registered.status.value, registered.bodyAsText())
            assertTrue("split nesting depth 4 exceeds split.maxDepth 3" in registered.bodyAsText(), registered.bodyAsText())

            val started = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody("""{"definition": $tooDeep, "payload": {}}""")
            }
            assertEquals(400, started.status.value, started.bodyAsText())
            assertTrue("split nesting depth 4 exceeds split.maxDepth 3" in started.bodyAsText(), started.bodyAsText())

            val atTheLimit = client.post("/workflows/definitions") {
                contentType(ContentType.Application.Json)
                setBody(v2Definition("deep-enough-${UUID.randomUUID()}", nestedSplitNodes(3), "s1"))
            }
            assertEquals(200, atTheLimit.status.value, atTheLimit.bodyAsText())
        }
    }

    @Test
    fun `split maxDepth comes from the config`() {
        apiOnlyAgainstTestContainers()
        set("split.maxDepth", "1")
        testApplication {
            application { module() }
            val started = client.post("/workflows/run") {
                contentType(ContentType.Application.Json)
                setBody(v2Run("max-depth-1-${UUID.randomUUID()}", nestedSplitNodes(2), "s1"))
            }
            assertEquals(400, started.status.value, started.bodyAsText())
            assertTrue("split nesting depth 2 exceeds split.maxDepth 1" in started.bodyAsText(), started.bodyAsText())
        }
    }
}
