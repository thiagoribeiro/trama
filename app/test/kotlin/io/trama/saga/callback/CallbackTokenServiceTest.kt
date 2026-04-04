package run.trama.saga.callback

import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CallbackTokenServiceTest {
    private val secret = "test-secret-key"
    private val service = CallbackTokenService(secret, "default")
    private val executionId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val nodeId = "payment"

    @Test
    fun `generate and validate round trip succeeds`() {
        val meta = service.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val token = service.tokenString(meta)

        val parsed = service.parseToken(token)
        assertNotNull(parsed)
        assertEquals(meta.nonce, parsed.nonce)
        assertEquals(0, parsed.attempt)
        assertEquals(meta.expiresAt.epochSecond, parsed.expiresAt.epochSecond)

        val result = service.validate(executionId, nodeId, 0, parsed.nonce, parsed.expiresAt, parsed.signature)
        assertIs<CallbackValidationResult.Valid>(result)
    }

    @Test
    fun `validate returns 401 for wrong signature`() {
        val meta = service.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val result = service.validate(executionId, nodeId, 0, meta.nonce, meta.expiresAt, "bad-signature")
        val invalid = assertIs<CallbackValidationResult.Invalid>(result)
        assertEquals(401, invalid.httpStatus)
    }

    @Test
    fun `validate returns 410 for expired token`() {
        val meta = service.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val pastExpiry = Instant.now().minusSeconds(1)
        val result = service.validate(executionId, nodeId, 0, meta.nonce, pastExpiry, meta.signature)
        val invalid = assertIs<CallbackValidationResult.Invalid>(result)
        assertEquals(410, invalid.httpStatus)
    }

    @Test
    fun `validate fails when nodeId differs`() {
        val meta = service.generate(executionId, nodeId, attempt = 0, timeoutMillis = 60_000)
        val result = service.validate(executionId, "other-node", 0, meta.nonce, meta.expiresAt, meta.signature)
        assertIs<CallbackValidationResult.Invalid>(result)
    }

    @Test
    fun `parseToken returns null for malformed token`() {
        assertNull(service.parseToken("not-a-token"))
        assertNull(service.parseToken(""))
        assertNull(service.parseToken("a:b:c"))  // only 3 parts
    }

    @Test
    fun `parseToken handles uuid nonce with hyphens`() {
        val nonce = UUID.randomUUID().toString()
        val attempt = 2
        val epochSecond = Instant.now().plusSeconds(60).epochSecond
        val token = "$nonce:$attempt:$epochSecond:somesignature"
        val parsed = service.parseToken(token)
        assertNotNull(parsed)
        assertEquals(nonce, parsed.nonce)
        assertEquals(attempt, parsed.attempt)
        assertEquals(epochSecond, parsed.expiresAt.epochSecond)
        assertEquals("somesignature", parsed.signature)
    }

    @Test
    fun `different secrets produce different signatures`() {
        val service2 = CallbackTokenService("different-secret", "default")
        val meta1 = service.generate(executionId, nodeId, 0, 60_000)
        val result = service2.validate(executionId, nodeId, 0, meta1.nonce, meta1.expiresAt, meta1.signature)
        assertIs<CallbackValidationResult.Invalid>(result)
    }
}
