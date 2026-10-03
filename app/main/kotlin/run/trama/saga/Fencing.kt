package run.trama.saga

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.currentCoroutineContext

/**
 * This worker's hold on a claimed queue item. The claim's in-flight deadline lives in Redis and is
 * pushed forward by the consumer's heartbeat; [deadlineMillis] mirrors the last deadline this
 * worker set. Once it passes (a long GC pause, SIGSTOP, host suspend) another worker may already
 * have taken the item over, so this worker must stop acting on it. The check is local: no round
 * trip on the hot path.
 *
 * It travels in the coroutine context of the work it guards, so [ensureLease] can be called from
 * deep inside the executor without threading it through every signature. Code running without a
 * lease (callbacks, scanners, tests) is never fenced by it.
 */
class ClaimLease(
    @Volatile var deadlineMillis: Long,
    /** Safety margin before the deadline: a claim this close to expiring is treated as gone. */
    private val marginMillis: Long,
) : AbstractCoroutineContextElement(Key) {
    /** Set by the heartbeat when Redis shows the claim is no longer this worker's. */
    @Volatile var lost: Boolean = false

    fun isHeld(nowMillis: Long = System.currentTimeMillis()): Boolean =
        !lost && nowMillis < deadlineMillis - marginMillis

    companion object Key : CoroutineContext.Key<ClaimLease>
}

/** Thrown when the current claim's lease is no longer held; the work must be dropped, not failed. */
class LeaseLostException : RuntimeException("claim lease lost") {
    override fun fillInStackTrace(): Throwable = this
}

/** Throws [LeaseLostException] when running under a [ClaimLease] that is no longer held. */
suspend fun ensureLease() {
    val lease = currentCoroutineContext()[ClaimLease] ?: return
    if (!lease.isHeld()) throw LeaseLostException()
}

/**
 * Thrown when a checkpoint write finds the execution's persisted sequence no longer matches the
 * one this worker started from (or the execution is already terminal): someone else advanced it.
 */
class StaleCheckpointException(val executionId: java.util.UUID) :
    RuntimeException("stale checkpoint for execution $executionId") {
    override fun fillInStackTrace(): Throwable = this
}
