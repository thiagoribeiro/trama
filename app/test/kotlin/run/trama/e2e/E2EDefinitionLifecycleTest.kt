package run.trama.e2e

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.ktor.client.request.delete
import io.ktor.client.statement.bodyAsText
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Stored-definition API beyond what E2EDefinitionApiTest covers: list, name/version lookup, PUT, DELETE, v2. */
class E2EDefinitionLifecycleTest {
    private lateinit var wm: WireMockServer

    @BeforeEach
    fun setUp() {
        assumeDocker()
        wm = WireMockServer(wireMockConfig().dynamicPort()).also { it.start() }
        wm.stubSuccessStep()
    }

    @AfterEach
    fun tearDown() {
        if (::wm.isInitialized) wm.stop()
    }

    private fun svc(path: String) = "http://localhost:${wm.port()}$path"

    private fun v1Def(name: String, version: String = "v1") = mapOf(
        "name" to name,
        "version" to version,
        "failureHandling" to mapOf("type" to "retry", "maxAttempts" to 0, "delayMillis" to 10),
        "steps" to listOf(
            mapOf(
                "name" to "charge",
                "up" to httpCallMap(svc("/step/charge"), mapOf("order" to "{{payload.orderId}}")),
                "down" to httpCallMap(svc("/step/undo")),
            ),
        ),
    )

    @Test
    fun `v2 definition is stored, listed and fetched by name and version`() = e2eTest {
        val name = uniqueName("def-v2")
        val def = v2DefinitionMap(name, listOf(taskNodeMap("a", svc("/step/a"))))

        val created = client.postJson("/workflows/definitions", def)
        assertEquals(200, created.status.value)
        val (_, createdBody) = client.getJson("/workflows/definitions/$name/v1")
        val id = createdBody!!.jsonObject["id"]!!.jsonPrimitive.content
        assertTrue(createdBody.jsonObject["definition"]!!.jsonObject.containsKey("nodes"), "v2 graph must round-trip")

        val (listStatus, list) = client.getJson("/workflows/definitions")
        assertEquals(200, listStatus)
        assertTrue(list!!.jsonArray.any { it.jsonObject["id"]!!.jsonPrimitive.content == id })
    }

    @Test
    fun `running a stored v2 definition is rejected as not yet supported`() = e2eTest {
        val name = uniqueName("def-v2-run")
        client.postJson("/workflows/definitions", v2DefinitionMap(name, listOf(taskNodeMap("a", svc("/step/a")))))

        val resp = client.postJson("/workflows/definitions/$name/v1/run", mapOf("payload" to emptyMap<String, Any>()))
        assertEquals(422, resp.status.value)
    }

    @Test
    fun `running a stored v1 definition passes the payload to templates`() = e2eTest {
        val name = uniqueName("def-run-payload")
        client.postJson("/workflows/definitions", v1Def(name))

        val resp = client.postJson("/workflows/definitions/$name/v1/run", mapOf("payload" to mapOf("orderId" to "ord-55")))
        assertEquals(200, resp.status.value)
        val id = testJson.parseToJsonElement(resp.bodyAsText()).jsonObject["id"]!!.jsonPrimitive.content
        assertEquals("SUCCEEDED", awaitSagaTerminal(client, id)["status"]?.jsonPrimitive?.content)

        val body = testJson.parseToJsonElement(wm.requestsTo("/step/charge").single().bodyAsString).jsonObject
        assertEquals("ord-55", body["order"]?.jsonPrimitive?.content)
    }

    @Test
    fun `PUT creates with an explicit id and is insert-only`() = e2eTest {
        val id = UUID.randomUUID()
        val name = uniqueName("def-put")

        val first = client.putJson("/workflows/definitions/$id", v1Def(name))
        assertEquals(200, first.status.value)
        val (_, fetched) = client.getJson("/workflows/definitions/$id")
        assertEquals(name, fetched!!.jsonObject["name"]!!.jsonPrimitive.content)

        // Documented in openapi.json: "This endpoint inserts only; it returns 409 if the id or name/version already exists."
        assertEquals(409, client.putJson("/workflows/definitions/$id", v1Def(name, "v2")).status.value)
        assertEquals(409, client.putJson("/workflows/definitions/${UUID.randomUUID()}", v1Def(name)).status.value)
        assertEquals(400, client.putJson("/workflows/definitions/not-a-uuid", v1Def(name)).status.value)
    }

    @Test
    fun `DELETE removes the definition from every lookup path`() = e2eTest {
        val name = uniqueName("def-del")
        client.postJson("/workflows/definitions", v1Def(name))
        val id = client.getJson("/workflows/definitions/$name/v1").second!!.jsonObject["id"]!!.jsonPrimitive.content

        assertEquals(204, client.delete("/workflows/definitions/$id").status.value)

        assertEquals(204, client.getJson("/workflows/definitions/$id").first)
        assertEquals(204, client.getJson("/workflows/definitions/$name/v1").first)
        assertEquals(204, client.postJson("/workflows/definitions/$name/v1/run", mapOf("payload" to emptyMap<String, Any>())).status.value)
        assertEquals(204, client.delete("/workflows/definitions/$id").status.value, "deleting twice is a no-op")
        assertEquals(400, client.delete("/workflows/definitions/nope").status.value)
    }

    @Test
    fun `invalid definitions are rejected with 400 and a list of errors`() = e2eTest {
        val broken = v2DefinitionMap(
            uniqueName("def-bad"),
            listOf(taskNodeMap("a", svc("/step/a"), next = "does-not-exist")),
        )
        val resp = client.postJson("/workflows/definitions", broken)
        assertEquals(400, resp.status.value)
        val errors = testJson.parseToJsonElement(resp.bodyAsText()).jsonObject["errors"]
        assertNotNull(errors)
        assertTrue(errors.jsonArray.isNotEmpty())

        val inlineResp = client.postJson("/workflows/run", mapOf("definition" to broken))
        assertEquals(400, inlineResp.status.value)
    }

    @Test
    fun `unknown definitions return 204`() = e2eTest {
        assertEquals(204, client.getJson("/workflows/definitions/${UUID.randomUUID()}").first)
        assertEquals(204, client.getJson("/workflows/definitions/nope-${UUID.randomUUID()}/v1").first)
        assertEquals(204, client.postJson("/workflows/definitions/nope-${UUID.randomUUID()}/v1/run", mapOf("payload" to emptyMap<String, Any>())).status.value)
    }
}
