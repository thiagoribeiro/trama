package run.trama.saga

import com.ensarsarajcic.kotlinx.serialization.msgpack.MsgPack
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.json.JsonPrimitive

class MsgPackRoundTripTest {
    @Test
    fun `saga execution msgpack roundtrip`() {
        val msgPack = MsgPack()
        val definition = SagaDefinition(
            name = "payment",
            version = "1",
            failureHandling = FailureHandling.Backoff(
                maxAttempts = 3,
                initialDelayMillis = 100,
                maxDelayMillis = 1000,
            ),
            steps = listOf(
                SagaStep(
                    name = "charge",
                    up = HttpCall(
                        url = TemplateString("http://pay/charge"),
                        verb = HttpVerb.POST,
                    ),
                    down = HttpCall(
                        url = TemplateString("http://pay/refund"),
                        verb = HttpVerb.POST,
                    ),
                )
            ),
            onSuccessCallback = null,
            onFailureCallback = null,
        )
        val original = SagaExecution(
            definition = definition,
            id = UUID.randomUUID(),
            startedAt = Instant.parse("2025-01-10T12:00:00Z"),
            currentStepIndex = 0,
            state = ExecutionState.InProgress(activeNodeId = null, phase = ExecutionPhase.UP),
            payload = mapOf("orderId" to PayloadValue(JsonPrimitive("A1"))),
        )

        val bytes = msgPack.encodeToByteArray(SagaExecution.serializer(), original)
        val decoded: SagaExecution = msgPack.decodeFromByteArray(SagaExecution.serializer(), bytes)

        assertEquals(original.id, decoded.id)
        assertEquals(original.definition.name, decoded.definition.name)
        assertEquals(original.definition.steps.size, decoded.definition.steps.size)
        assertEquals(original.startedAt, decoded.startedAt)
        assertEquals(original.state::class, decoded.state::class)
    }
}
