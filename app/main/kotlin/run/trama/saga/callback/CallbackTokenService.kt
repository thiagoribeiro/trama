package run.trama.saga.callback

import java.time.Instant
import java.util.Base64
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

data class CallbackMeta(
    val nonce: String,
    val attempt: Int,
    val expiresAt: Instant,
    val signature: String,
    val kid: String,
)

sealed class CallbackValidationResult {
    data object Valid : CallbackValidationResult()
    data class Invalid(val reason: String, val httpStatus: Int) : CallbackValidationResult()
}

/**
 * Signs and validates HMAC-SHA256 callback tokens.
 *
 * Token wire format (passed as `X-Callback-Token` header):
 *   `{nonce}:{attempt}:{expiresAtEpochSeconds}:{base64url-hmac}`
 *
 * HMAC message: `{executionId}:{nodeId}:{attempt}:{nonce}:{expiresAtEpochSeconds}`
 */
class CallbackTokenService(
    private val hmacSecret: String,
    private val kid: String = "default",
) {
    // Pre-computed key spec — avoids re-creating SecretKeySpec on every sign/validate call
    private val secretKeySpec = SecretKeySpec(hmacSecret.toByteArray(Charsets.UTF_8), "HmacSHA256")

    // ThreadLocal Mac instance — Mac is not thread-safe, but ThreadLocal avoids repeated getInstance() calls
    private val macThreadLocal: ThreadLocal<Mac> = ThreadLocal.withInitial {
        Mac.getInstance("HmacSHA256").also { it.init(secretKeySpec) }
    }

    fun generate(
        executionId: UUID,
        nodeId: String,
        attempt: Int,
        timeoutMillis: Long,
    ): CallbackMeta {
        val nonce = UUID.randomUUID().toString()
        val expiresAt = Instant.now().plusMillis(timeoutMillis)
        val signature = sign(executionId, nodeId, attempt, nonce, expiresAt)
        return CallbackMeta(nonce = nonce, attempt = attempt, expiresAt = expiresAt, signature = signature, kid = kid)
    }

    fun tokenString(meta: CallbackMeta): String =
        "${meta.nonce}:${meta.attempt}:${meta.expiresAt.epochSecond}:${meta.signature}"

    fun validate(
        executionId: UUID,
        nodeId: String,
        attempt: Int,
        nonce: String,
        expiresAt: Instant,
        incomingSignature: String,
    ): CallbackValidationResult {
        if (Instant.now().isAfter(expiresAt)) {
            return CallbackValidationResult.Invalid("callback token expired", 410)
        }
        val expected = sign(executionId, nodeId, attempt, nonce, expiresAt)
        if (!constantTimeEquals(expected, incomingSignature)) {
            return CallbackValidationResult.Invalid("invalid callback token signature", 401)
        }
        return CallbackValidationResult.Valid
    }

    fun parseToken(token: String): ParsedToken? {
        val parts = token.split(":")
        if (parts.size < 4) return null
        // nonce may contain hyphens (UUID), attempt and epoch are last two numeric parts, signature is last
        // format: {nonce}:{attempt}:{epochSeconds}:{signature}
        val signature = parts.last()
        val epochSecond = parts[parts.size - 2].toLongOrNull() ?: return null
        val attempt = parts[parts.size - 3].toIntOrNull() ?: return null
        val nonce = parts.dropLast(3).joinToString(":")
        if (nonce.isBlank()) return null
        return ParsedToken(nonce = nonce, attempt = attempt, expiresAt = Instant.ofEpochSecond(epochSecond), signature = signature)
    }

    data class ParsedToken(
        val nonce: String,
        val attempt: Int,
        val expiresAt: Instant,
        val signature: String,
    )

    private fun sign(executionId: UUID, nodeId: String, attempt: Int, nonce: String, expiresAt: Instant): String {
        val message = "$executionId:$nodeId:$attempt:$nonce:${expiresAt.epochSecond}"
        val mac = macThreadLocal.get()
        mac.reset()
        return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(message.toByteArray(Charsets.UTF_8)))
    }

    private fun constantTimeEquals(a: String, b: String): Boolean {
        if (a.length != b.length) return false
        var result = 0
        for (i in a.indices) result = result or (a[i].code xor b[i].code)
        return result == 0
    }
}
