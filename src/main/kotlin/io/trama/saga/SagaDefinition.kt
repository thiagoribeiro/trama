package run.trama.saga

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

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

@Serializable(with = TemplateStringSerializer::class)
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

object TemplateStringSerializer : KSerializer<TemplateString> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("TemplateString", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: TemplateString) {
        if (encoder is JsonEncoder) {
            encoder.encodeJsonElement(JsonObject(mapOf("value" to JsonPrimitive(value.value))))
            return
        }
        encoder.encodeString(value.value)
    }

    override fun deserialize(decoder: Decoder): TemplateString {
        if (decoder is JsonDecoder) {
            val element = decoder.decodeJsonElement()
            return TemplateString(readTemplateValue(element))
        }
        return TemplateString(decoder.decodeString())
    }

    private fun readTemplateValue(element: JsonElement): String {
        if (element is JsonPrimitive && element.isString) {
            return element.content
        }
        if (element is JsonObject) {
            val wrapped = element["value"]
            if (wrapped != null && wrapped is JsonPrimitive && wrapped.isString) {
                return wrapped.content
            }
        }
        return Json.encodeToString(JsonElement.serializer(), element)
    }
}
