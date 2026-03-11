package run.trama.app

import io.ktor.client.request.get
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals

class ApplicationTest {
    @Test
    fun healthz() = testApplication {
        System.setProperty("runtime.enabled", "false")
        application { module() }
        val response = client.get("/healthz")
        assertEquals(200, response.status.value)
    }
}
