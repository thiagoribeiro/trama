package run.trama.saga.redis

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.math.min

/**
 * Bounds how many claimed executions a process holds but has not finished: claimers take permits
 * before claiming and workers give one back per finished item. Claiming more than the workers can
 * run only made the surplus wait in memory (head-of-line blocking) and, on a crash, wait out the
 * whole claim lease before anyone else could run it.
 */
class ClaimPermits(total: Int) {
    private val available = AtomicInteger(total.coerceAtLeast(1))
    private val signal = Channel<Unit>(Channel.CONFLATED)

    /**
     * Takes between 1 and [max] permits, suspending while none are free. [onWait] runs about once a
     * second while waiting, so a caller can report that it is alive but saturated. Returns 0, having
     * taken nothing, once [active] turns false (the caller is shutting down).
     */
    suspend fun acquireUpTo(max: Int, active: () -> Boolean = { true }, onWait: () -> Unit = {}): Int {
        while (true) {
            if (!active()) return 0
            val free = available.get()
            if (free > 0) {
                val take = min(free, max)
                if (available.compareAndSet(free, free - take)) {
                    // Another claimer may be waiting for what is left over.
                    if (free > take) signal.trySend(Unit)
                    return take
                }
                continue
            }
            if (withTimeoutOrNull(1_000) { signal.receive() } == null) onWait()
        }
    }

    fun release(count: Int = 1) {
        if (count <= 0) return
        available.addAndGet(count)
        signal.trySend(Unit)
    }

    fun available(): Int = available.get()

    companion object {
        /** For consumers driven without a processor (tests): never limits. */
        val UNBOUNDED: ClaimPermits get() = ClaimPermits(Int.MAX_VALUE / 2)
    }
}
