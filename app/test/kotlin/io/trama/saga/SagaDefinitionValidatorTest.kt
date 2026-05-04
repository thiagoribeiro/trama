package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SagaDefinitionValidatorTest {
    @Test
    fun `valid definition passes`() {
        val def = SagaDefinition(
            name = "order-flow",
            version = "1",
            failureHandling = FailureHandling.Retry(maxAttempts = 3, delayMillis = 1000),
            steps = listOf(
                SagaStep(
                    name = "reserve",
                    up = HttpCall(
                        url = TemplateString("http://service/reserve/{{sagaId}}"),
                        verb = HttpVerb.POST,
                        headers = mapOf("X-Req" to TemplateString("{{sagaId}}")),
                        body = TemplateString("{\"id\":\"{{sagaId}}\"}"),
                    ),
                    down = HttpCall(
                        url = TemplateString("http://service/reserve/{{sagaId}}"),
                        verb = HttpVerb.DELETE,
                    ),
                )
            ),
            onSuccessCallback = null,
            onFailureCallback = null,
        )

        val errors = SagaDefinitionValidator.validate(def)
        assertTrue(errors.isEmpty(), "Expected no validation errors, got: $errors")
    }

    @Test
    fun `invalid definition returns errors`() {
        val def = SagaDefinition(
            name = " ",
            version = "",
            failureHandling = FailureHandling.Retry(maxAttempts = 0, delayMillis = 0),
            steps = listOf(
                SagaStep(
                    name = "",
                    up = HttpCall(
                        url = TemplateString(""),
                        verb = HttpVerb.POST,
                        headers = mapOf("" to TemplateString("")),
                        body = TemplateString(""),
                        successStatusCodes = emptySet(),
                    ),
                    down = HttpCall(
                        url = TemplateString(""),
                        verb = HttpVerb.DELETE,
                        successStatusCodes = setOf(42),
                    ),
                )
            ),
            onSuccessCallback = HttpCall(
                url = TemplateString(""),
                verb = HttpVerb.POST,
                successStatusCodes = emptySet(),
            ),
            onFailureCallback = null,
        )

        val errors = SagaDefinitionValidator.validate(def)
        assertTrue(errors.isNotEmpty())
        assertTrue(errors.any { it.contains("name must not be blank") })
        assertTrue(errors.any { it.contains("version must not be blank") })
        assertTrue(errors.any { it.contains("steps[0].name must not be blank") })
        assertTrue(errors.any { it.contains("steps[0].up.url must not be blank") })
        assertTrue(errors.any { it.contains("steps[0].up.successStatusCodes must not be empty") })
        assertTrue(errors.any { it.contains("steps[0].down.successStatusCodes contains invalid code: 42") })
        assertTrue(errors.any { it.contains("onSuccessCallback.url must not be blank") })
    }
}
