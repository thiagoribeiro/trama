package run.trama.saga.redis

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

class RendezvousShardAllocator(
    private val localPodId: String,
    private val virtualShardCount: Int,
) {
    private val ownedShards = AtomicReference<List<Int>>(emptyList())
    private val activePods = AtomicReference<List<String>>(emptyList())

    fun updatePods(pods: Collection<String>) {
        val normalized = pods.filter { it.isNotBlank() }.distinct().sorted()
        activePods.set(normalized)
        val nextOwned =
            if (normalized.isEmpty()) {
                emptyList()
            } else {
                (0 until virtualShardCount).filter { shardId -> ownerFor(shardId, normalized) == localPodId }
            }
        ownedShards.set(nextOwned)
    }

    fun ownedShards(): List<Int> = ownedShards.get()

    fun activePods(): List<String> = activePods.get()

    internal fun ownerFor(shardId: Int, pods: List<String>): String? {
        if (pods.isEmpty()) return null

        var winner: String? = null
        var winningScore = 0L
        for (pod in pods) {
            val score = rendezvousScore(shardId, pod)
            if (winner == null || java.lang.Long.compareUnsigned(score, winningScore) > 0) {
                winner = pod
                winningScore = score
            }
        }
        return winner
    }

    private fun rendezvousScore(shardId: Int, podId: String): Long {
        val input = "$shardId:$podId".toByteArray(StandardCharsets.UTF_8)
        var hash = FNV64_OFFSET_BASIS
        for (byte in input) {
            hash = hash xor (byte.toLong() and 0xff)
            hash *= FNV64_PRIME
        }
        return hash
    }

    private companion object {
        const val FNV64_OFFSET_BASIS = -3750763034362895579L
        const val FNV64_PRIME = 1099511628211L
    }
}
