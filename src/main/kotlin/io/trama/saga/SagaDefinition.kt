package io.trama.saga

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class SagaDefinition(
    val name: String,
    val version: String,
    val failureHandling: FailureHandling,
    val steps: List<SagaStep>,
    val onSuccessCallback: HttpCall? = null,
    val onFailureCallback: HttpCall? = null,
)

@Serializable
data class SagaStep(
    val name: String,
    val up: HttpCall,
    val down: HttpCall,
)

@Serializable
data class HttpCall(
    val url: TemplateString,
    val verb: HttpVerb,
    val headers: Map<String, TemplateString> = emptyMap(),
    val body: TemplateString? = null,
    val successStatusCodes: Set<Int> = defaultSuccessStatusCodes(verb),
)

@Serializable
data class TemplateString(
    val value: String,
)

@Serializable
enum class HttpVerb {
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    HEAD,
    OPTIONS,
}

private fun defaultSuccessStatusCodes(verb: HttpVerb): Set<Int> =
    when (verb) {
        HttpVerb.GET,
        HttpVerb.HEAD,
        HttpVerb.OPTIONS,
        -> setOf(200)
        HttpVerb.POST -> setOf(200, 201, 202)
        HttpVerb.PUT,
        HttpVerb.PATCH,
        HttpVerb.DELETE,
        -> setOf(200, 202, 204)
    }

@Serializable
sealed class FailureHandling {
    @Serializable
    @SerialName("retry")
    data class Retry(
        val maxAttempts: Int,
        val delayMillis: Long,
    ) : FailureHandling()

    @Serializable
    @SerialName("backoff")
    data class Backoff(
        val maxAttempts: Int,
        val initialDelayMillis: Long,
        val maxDelayMillis: Long,
        val multiplier: Double = 2.0,
        val jitterRatio: Double = 0.0,
    ) : FailureHandling()
}
