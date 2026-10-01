package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RetryPolicyTest {
    private val policy = DefaultRetryPolicy()

    @Test
    fun `first failure starts at attempt 1`() {
        val decision = policy.next(RetryState.None, FailureHandling.Retry(maxAttempts = 3, delayMillis = 50))
        assertTrue(decision.shouldRetry)
        assertEquals(1, decision.attempt)
        assertEquals(50, decision.delayMillis)
    }

    @Test
    fun `fixed retry allows exactly maxAttempts retries`() {
        val handling = FailureHandling.Retry(maxAttempts = 2, delayMillis = 10)
        assertTrue(policy.next(RetryState.Applying(1, 10), handling).shouldRetry)
        val exhausted = policy.next(RetryState.Applying(2, 10), handling)
        assertFalse(exhausted.shouldRetry)
        assertEquals(3, exhausted.attempt)
        assertEquals(0, exhausted.delayMillis)
    }

    @Test
    fun `maxAttempts zero never retries`() {
        assertFalse(policy.next(RetryState.None, FailureHandling.Retry(0, 10)).shouldRetry)
    }

    @Test
    fun `backoff without jitter grows geometrically`() {
        val handling = FailureHandling.Backoff(maxAttempts = 5, initialDelayMillis = 100, maxDelayMillis = 100_000, multiplier = 2.0)
        val delays = listOf(RetryState.None, RetryState.Applying(1, 0), RetryState.Applying(2, 0), RetryState.Applying(3, 0))
            .map { policy.next(it, handling).delayMillis }
        assertEquals(listOf(100L, 200L, 400L, 800L), delays)
    }

    @Test
    fun `backoff is capped at maxDelayMillis`() {
        val handling = FailureHandling.Backoff(maxAttempts = 10, initialDelayMillis = 100, maxDelayMillis = 300, multiplier = 3.0)
        assertEquals(300, policy.next(RetryState.Applying(4, 0), handling).delayMillis)
    }

    @Test
    fun `backoff stops after maxAttempts`() {
        val handling = FailureHandling.Backoff(maxAttempts = 2, initialDelayMillis = 100, maxDelayMillis = 1000)
        assertFalse(policy.next(RetryState.Applying(2, 0), handling).shouldRetry)
    }

    @Test
    fun `backoff jitter stays within ratio and never goes negative`() {
        val handling = FailureHandling.Backoff(maxAttempts = 5, initialDelayMillis = 1000, maxDelayMillis = 10_000, jitterRatio = 0.25)
        repeat(500) {
            val d = policy.next(RetryState.None, handling).delayMillis
            assertTrue(d in 750..1250, "delay $d outside ±25% of 1000")
        }
        val wild = FailureHandling.Backoff(maxAttempts = 5, initialDelayMillis = 10, maxDelayMillis = 10, jitterRatio = 5.0)
        repeat(500) {
            assertTrue(policy.next(RetryState.None, wild).delayMillis >= 0)
        }
    }
}
