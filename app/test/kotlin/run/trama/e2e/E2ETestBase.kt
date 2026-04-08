package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.any
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import run.trama.app.module
import java.util.UUID

/**
 * Singleton containers started once per JVM run; shared across all E2E test classes.
 * System properties are configured here so every [testApplication] call that sets
 * [runtime.enabled=true] gets the correct database and Redis coordinates.
 */
object E2EContainers {
    val postgres: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:15-alpine").also { it.start() }
    }

    val redis: GenericContainer<*> by lazy {
        GenericContainer(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .also { it.start() }
    }

    /** Must be called before the first test that needs a live runtime. */
    fun configureSystemProperties(callbackBaseUrl: String = "http://test-callback-host") {
        System.setProperty("database.host", postgres.host)
        System.setProperty("database.port", postgres.firstMappedPort.toString())
        System.setProperty("database.database", postgres.databaseName)
        System.setProperty("database.user", postgres.username)
        System.setProperty("database.password", postgres.password)
        System.setProperty("redis.url", "redis://${redis.host}:${redis.getMappedPort(6379)}")
        System.setProperty("runtime.enabled", "true")
        System.setProperty("runtime.workerCount", "2")
        System.setProperty("telemetry.enabled", "false")
        System.setProperty("metrics.enabled", "false")
        System.setProperty("runtime.callback.baseUrl", callbackBaseUrl)
        System.setProperty("runtime.callback.hmacSecret", "test-e2e-hmac-secret")
        System.setProperty("runtime.callback.hmacKid", "test")
    }
}

// ── JSON helper ───────────────────────────────────────────────────────────────

val testJson = Json { ignoreUnknownKeys = true; encodeDefaults = true; explicitNulls = false }

/**
 * Recursively converts a Kotlin [Map]/[List]/primitive to a [JsonElement].
 * Used for serializing ad-hoc test-definition maps whose values are typed as [Any?].
 */
fun Any?.toJsonElement(): JsonElement = when (this) {
    null -> JsonNull
    is Boolean -> JsonPrimitive(this)
    is Number -> JsonPrimitive(this)
    is String -> JsonPrimitive(this)
    is Map<*, *> -> JsonObject(entries.associate { (k, v) -> k.toString() to v.toJsonElement() })
    is Iterable<*> -> JsonArray(map { it.toJsonElement() })
    else -> JsonPrimitive(toString())
}

// ── WireMock helpers ──────────────────────────────────────────────────────────

fun WireMockServer.stubSuccessStep(path: String = "/step/.*") {
    stubFor(
        post(urlPathMatching(path))
            .willReturn(
                aResponse().withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"status":"ok"}""")
            )
    )
}

fun WireMockServer.stubFailStep(path: String, statusCode: Int = 500) {
    stubFor(
        post(urlPathEqualTo(path))
            .willReturn(aResponse().withStatus(statusCode).withBody("""{"error":"forced failure"}"""))
    )
}

fun WireMockServer.stubAsyncStep(path: String) {
    stubFor(
        post(urlPathEqualTo(path))
            .willReturn(
                aResponse().withStatus(202)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"status":"accepted"}""")
            )
    )
}

fun WireMockServer.stubCallbackEndpoint() {
    stubFor(
        any(urlPathMatching("/callback/.*"))
            .willReturn(aResponse().withStatus(200).withBody("""{"status":"ok"}"""))
    )
}

/**
 * Reads the body of the first request received at [stepPath] and extracts the
 * [fieldName] from the JSON body. Used to retrieve callbackToken and callbackUrl
 * from async step requests captured by WireMock.
 */
fun WireMockServer.extractBodyField(stepPath: String, fieldName: String): String {
    val requests = findAll(postRequestedFor(urlPathEqualTo(stepPath)))
    check(requests.isNotEmpty()) { "No request recorded at $stepPath" }
    val body = testJson.parseToJsonElement(requests.first().bodyAsString).jsonObject
    return body[fieldName]?.jsonPrimitive?.content
        ?: error("Field '$fieldName' not found in captured request body at $stepPath")
}

// ── Polling helpers ───────────────────────────────────────────────────────────

private val terminalStatuses = setOf("SUCCEEDED", "FAILED", "CORRUPTED")

/**
 * Polls [GET /sagas/{id}] until the saga reaches a terminal status or [timeoutMs] elapses.
 * Returns the final JSON body.
 */
suspend fun awaitSagaTerminal(client: HttpClient, sagaId: String, timeoutMs: Long = 30_000): JsonObject {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
        val resp = client.get("/sagas/$sagaId")
        if (resp.status.value != 204) {
            val body = testJson.parseToJsonElement(resp.bodyAsText()).jsonObject
            val status = body["status"]?.jsonPrimitive?.content
            if (status in terminalStatuses) return body
        }
        delay(200)
    }
    error("Saga $sagaId did not reach terminal status within ${timeoutMs}ms")
}

/**
 * Polls [GET /sagas/{id}] until the saga reaches [expectedStatus] or [timeoutMs] elapses.
 */
suspend fun awaitSagaStatus(
    client: HttpClient,
    sagaId: String,
    expectedStatus: String,
    timeoutMs: Long = 30_000,
): JsonObject {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
        val resp = client.get("/sagas/$sagaId")
        if (resp.status.value != 204) {
            val body = testJson.parseToJsonElement(resp.bodyAsText()).jsonObject
            if (body["status"]?.jsonPrimitive?.content == expectedStatus) return body
        }
        delay(200)
    }
    error("Saga $sagaId did not reach status '$expectedStatus' within ${timeoutMs}ms")
}

// ── Test application builder ──────────────────────────────────────────────────

/**
 * Launches a full-stack [testApplication] with [runtime.enabled=true] against
 * real Postgres and Redis containers. The [block] is executed inside the running
 * application.
 *
 * Each call re-sets the system properties so container coordinates are always
 * up-to-date (containers start lazily on first access).
 */
fun e2eTest(
    wmPort: Int? = null,
    block: suspend ApplicationTestBuilder.() -> Unit,
) {
    E2EContainers.configureSystemProperties(
        callbackBaseUrl = if (wmPort != null) "http://localhost:$wmPort" else "http://test-callback-host"
    )
    testApplication {
        application { module() }
        block()
    }
}

// ── Definition builders ───────────────────────────────────────────────────────

fun uniqueName(prefix: String = "e2e"): String = "$prefix-${UUID.randomUUID().toString().take(8)}"
