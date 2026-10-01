package run.trama.app

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject

/**
 * Every HTTP route the server declares must be documented in openapi.json (the site's API page
 * renders it live). Several list/step/callback routes had been missing.
 */
class OpenApiRoutesContractTest {

    @Test
    fun `every route in Application kt is documented in openapi json`() {
        val routes = Regex("""\b(get|post|put|delete|patch)\("(/[^"]*)"\)""")
            .findAll(File("app/main/kotlin/run/trama/app/Application.kt").readText())
            .map { "${it.groupValues[1].uppercase()} ${it.groupValues[2]}" }
            .toSet()
        val paths = Json.parseToJsonElement(File("openapi.json").readText()).jsonObject.getValue("paths").jsonObject
        val documented = paths.flatMap { (path, ops) -> ops.jsonObject.keys.map { "${it.uppercase()} $path" } }.toSet()

        assertTrue(routes.isNotEmpty(), "no routes found — did Application.kt move?")
        val missing = routes - documented
        assertTrue(missing.isEmpty(), "routes missing from openapi.json: $missing")
    }
}
