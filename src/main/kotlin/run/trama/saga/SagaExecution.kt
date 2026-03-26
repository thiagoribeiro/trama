package run.trama.saga

import java.time.Instant
import java.util.UUID
import kotlinx.serialization.SerialName
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive

@Serializable
data class SagaExecution(
    val definition: SagaDefinition,
    /** Non-null when this execution was created from a v2 node-graph definition. */
    val definitionV2: SagaDefinitionV2? = null,
    @Serializable(with = UuidAsStringSerializer::class)
    val id: UUID,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    val currentStepIndex: Int,
    val state: ExecutionState,
    val payload: Map<String, PayloadValue> = emptyMap(),
)

@Serializable
data class PayloadValue(
    @Serializable(with = JsonElementAsStringSerializer::class)
    val value: JsonElement,
)

@Serializable
sealed class ExecutionState {
    /**
     * Execution is progressing forward through nodes.
     *
     * [activeNodeId] is the node currently being executed (null = legacy format,
     * derive from [SagaExecution.currentStepIndex] + definition).
     * [compensationStack] lists node ids to compensate (front = next to run), built
     * up as nodes complete successfully.
     * [phase] is kept only for backward-compatible deserialization of pre-PR2 data.
     */
    @Serializable
    @SerialName("in_progress")
    data class InProgress(
        val activeNodeId: String? = null,
        val completedNodes: List<String> = emptyList(),
        val compensationStack: List<String> = emptyList(),
        /** Legacy field — present only in pre-PR2 serialized data. */
        val phase: ExecutionPhase? = null,
        val retry: RetryState = RetryState.None,
    ) : ExecutionState()

    /**
     * Execution failed forward; running compensations in reverse order.
     */
    @Serializable
    @SerialName("compensating")
    data class Compensating(
        /** Remaining nodes to compensate, front = next to run. */
        val compensationStack: List<String>,
        val completedNodes: List<String>,
        val failureReason: FailureReason,
        val retry: RetryState = RetryState.None,
    ) : ExecutionState()

    @Serializable
    @SerialName("failed")
    data class Failed(
        val reason: FailureReason? = null,
        val failedNodeId: String? = null,
        /** Legacy fields kept for backward-compat deserialization. */
        val phase: ExecutionPhase? = null,
        val failedStepIndex: Int? = null,
    ) : ExecutionState()

    /**
     * Execution sent an async HTTP request and is waiting for an external callback.
     * A timeout sentinel is enqueued in Redis ZSET with score = deadlineAt.toEpochMilli().
     */
    @Serializable
    @SerialName("waiting_callback")
    data class WaitingCallback(
        val nodeId: String,
        val attempt: Int,
        @Serializable(with = InstantAsStringSerializer::class)
        val deadlineAt: Instant,
        val nonce: String,
        val completedNodes: List<String>,
        val compensationStack: List<String>,
    ) : ExecutionState()

    @Serializable
    @SerialName("succeeded")
    data class Succeeded(
        @Serializable(with = InstantAsStringSerializer::class)
        val completedAt: Instant,
    ) : ExecutionState()
}

@Serializable
enum class ExecutionPhase {
    UP,
    DOWN,
    SWITCH,
}

@Serializable
sealed class RetryState {
    @Serializable
    @SerialName("none")
    data object None : RetryState()

    @Serializable
    @SerialName("applying")
    data class Applying(
        val attempt: Int,
        val nextDelayMillis: Long,
    ) : RetryState()
}

@Serializable
data class FailureReason(
    val message: String,
    val cause: String? = null,
)

@Serializable
data class SagaCreateResponse(
    val id: String,
)

@Serializable
data class ValidationErrorResponse(
    val errors: List<String>,
)

@Serializable
data class SagaStatusResponse(
    val id: String,
    val name: String,
    val version: String,
    val status: String,
    val failureDescription: String? = null,
    val callbackWarning: String? = null,
    val startedAt: String,
    val completedAt: String? = null,
    val updatedAt: String,
)

enum class SagaFinalStatus {
    SUCCEEDED,
    FAILED,
    CORRUPTED,
}

@Serializable
data class SagaRetryResponse(
    val id: String,
    val status: String,
)

@Serializable
data class SagaDefinitionCreateRequest(
    val name: String,
    val version: String,
    val failureHandling: FailureHandling,
    val steps: List<SagaStep>,
    val onSuccessCallback: HttpCall? = null,
    val onFailureCallback: HttpCall? = null,
)

@Serializable
data class SagaDefinitionResponse(
    val id: String,
    val name: String,
    val version: String,
    /** Raw definition JSON — supports both v1 (steps) and v2 (nodes) formats. */
    val definition: JsonElement,
    val createdAt: String,
    val updatedAt: String,
)

@Serializable
data class RunSagaRequest(
    val definition: SagaDefinition,
    val payload: Map<String, JsonElement> = emptyMap(),
)

@Serializable
data class RunStoredSagaRequest(
    val payload: Map<String, JsonElement> = emptyMap(),
)

object InstantAsStringSerializer : kotlinx.serialization.KSerializer<Instant> {
    override val descriptor = kotlinx.serialization.descriptors.PrimitiveSerialDescriptor(
        "InstantAsString",
        kotlinx.serialization.descriptors.PrimitiveKind.STRING
    )

    override fun serialize(encoder: kotlinx.serialization.encoding.Encoder, value: Instant) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: kotlinx.serialization.encoding.Decoder): Instant {
        return Instant.parse(decoder.decodeString())
    }
}

object UuidAsStringSerializer : kotlinx.serialization.KSerializer<UUID> {
    override val descriptor = kotlinx.serialization.descriptors.PrimitiveSerialDescriptor(
        "UuidAsString",
        kotlinx.serialization.descriptors.PrimitiveKind.STRING
    )

    override fun serialize(encoder: kotlinx.serialization.encoding.Encoder, value: UUID) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: kotlinx.serialization.encoding.Decoder): UUID {
        return UUID.fromString(decoder.decodeString())
    }
}

object JsonElementAsStringSerializer : KSerializer<JsonElement> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("JsonElementAsString", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: JsonElement) {
        encoder.encodeString(Json.encodeToString(JsonElement.serializer(), value))
    }

    override fun deserialize(decoder: Decoder): JsonElement {
        return Json.parseToJsonElement(decoder.decodeString())
    }
}

/**
 * Serializes a [JsonElement] as a JSON string for non-JSON formats (e.g., MsgPack),
 * but accepts both raw JSON elements AND pre-encoded strings when decoding from JSON.
 * Used for fields that arrive as JSON objects from the API but must survive MsgPack round-trips.
 */
object JsonElementFlexSerializer : KSerializer<JsonElement> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("JsonElementFlex", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: JsonElement) {
        encoder.encodeString(Json.encodeToString(JsonElement.serializer(), value))
    }

    override fun deserialize(decoder: Decoder): JsonElement {
        return if (decoder is JsonDecoder) {
            val element = decoder.decodeJsonElement()
            if (element is JsonPrimitive && element.isString) {
                Json.parseToJsonElement(element.content)
            } else {
                element
            }
        } else {
            Json.parseToJsonElement(decoder.decodeString())
        }
    }
}
