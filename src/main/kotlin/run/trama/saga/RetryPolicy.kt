package run.trama.saga

import kotlin.math.min
import kotlin.random.Random

interface RetryPolicy {
    fun next(retryState: RetryState, handling: FailureHandling): RetryDecision
}

class DefaultRetryPolicy : RetryPolicy {
    override fun next(retryState: RetryState, handling: FailureHandling): RetryDecision {
        val attempt = when (retryState) {
            RetryState.None -> 1
            is RetryState.Applying -> retryState.attempt + 1
        }

        return when (handling) {
            is FailureHandling.Retry -> {
                if (attempt > handling.maxAttempts) {
                    RetryDecision(false, attempt, 0)
                } else {
                    RetryDecision(true, attempt, handling.delayMillis)
                }
            }
            is FailureHandling.Backoff -> {
                if (attempt > handling.maxAttempts) {
                    RetryDecision(false, attempt, 0)
                } else {
                    val baseDelay = handling.initialDelayMillis * pow(handling.multiplier, attempt - 1)
                    val capped = min(baseDelay, handling.maxDelayMillis.toDouble())
                    val jitter = capped * handling.jitterRatio * (Random.nextDouble() - 0.5) * 2.0
                    val delay = (capped + jitter).toLong().coerceAtLeast(0)
                    RetryDecision(true, attempt, delay)
                }
            }
        }
    }

    private fun pow(base: Double, exp: Int): Double {
        var result = 1.0
        repeat(exp) { result *= base }
        return result
    }
}

data class RetryDecision(
    val shouldRetry: Boolean,
    val attempt: Int,
    val delayMillis: Long,
)
