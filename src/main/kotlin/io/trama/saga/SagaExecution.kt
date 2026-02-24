package io.trama.saga

import java.time.Instant
import java.util.UUID
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class SagaExecution(
    val definition: SagaDefinition,
    @Serializable(with = UuidAsStringSerializer::class)
    val id: UUID,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    val currentStepIndex: Int,
    val state: ExecutionState,
    val payload: Map<String, String> = emptyMap(),
)

@Serializable
sealed class ExecutionState {
    @Serializable
    @SerialName("in_progress")
    data class InProgress(
        val phase: ExecutionPhase,
        val retry: RetryState = RetryState.None,
    ) : ExecutionState()

    @Serializable
    @SerialName("failed")
    data class Failed(
        val phase: ExecutionPhase,
        val failedStepIndex: Int,
        val reason: FailureReason,
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
    val definition: SagaDefinition,
    val createdAt: String,
    val updatedAt: String,
)

@Serializable
data class RunSagaRequest(
    val definition: SagaDefinition,
    val payload: Map<String, String> = emptyMap(),
)

@Serializable
data class RunStoredSagaRequest(
    val payload: Map<String, String> = emptyMap(),
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
