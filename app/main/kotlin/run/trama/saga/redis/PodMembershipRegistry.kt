package run.trama.saga.redis

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.delay
import run.trama.telemetry.Metrics
import kotlin.time.Duration.Companion.milliseconds

class PodMembershipRegistry(
    private val redis: RedisCommandsProvider,
    private val membershipKey: String,
    private val podId: String,
    private val membershipTtlMillis: Long,
    private val heartbeatIntervalMillis: Long,
    private val refreshIntervalMillis: Long,
    private val allocator: RendezvousShardAllocator,
    private val metrics: Metrics,
) {
    private val membershipKeyBytes = membershipKey.toByteArray(StandardCharsets.UTF_8)
    private val lastRefreshAtMillis = AtomicLong(0L)
    private val lastHealthyPods = AtomicReference<List<String>>(emptyList())
    private val lastError = AtomicReference<String?>(null)

    suspend fun initialize() {
        heartbeatOnce()
        refreshOnce()
    }

    suspend fun unregister() {
        redis.withCommands { commands ->
            commands.zrem(membershipKeyBytes, podId.toByteArray(StandardCharsets.UTF_8))
        }
        val pods = lastHealthyPods.get().filterNot { it == podId }
        allocator.updatePods(pods)
        lastHealthyPods.set(pods)
        metrics.setRedisActivePods(pods.size.toLong())
        metrics.setRedisOwnedShards(allocator.ownedShards().size.toLong())
    }

    suspend fun runHeartbeatLoop() {
        while (true) {
            runCatching { heartbeatOnce() }
                .onFailure { lastError.set(it.message ?: "heartbeat_failed") }
            delay(heartbeatIntervalMillis.milliseconds)
        }
    }

    suspend fun runRefreshLoop() {
        while (true) {
            runCatching { refreshOnce() }
                .onFailure { lastError.set(it.message ?: "refresh_failed") }
            delay(refreshIntervalMillis.milliseconds)
        }
    }

    fun readiness(): ReadinessStatus {
        val now = System.currentTimeMillis()
        val refreshAt = lastRefreshAtMillis.get()
        if (refreshAt == 0L) {
            return ReadinessStatus(false, "membership_not_initialized")
        }
        val refreshAge = now - refreshAt
        metrics.setRedisMembershipRefreshAgeMillis(refreshAge)
        if (refreshAge > membershipTtlMillis) {
            return ReadinessStatus(false, "membership_stale")
        }
        val pods = lastHealthyPods.get()
        if (podId !in pods) {
            return ReadinessStatus(false, "pod_not_registered")
        }
        val error = lastError.get()
        if (error != null && refreshAge > refreshIntervalMillis * 2) {
            return ReadinessStatus(false, error)
        }
        return ReadinessStatus(true, "ready")
    }

    private suspend fun heartbeatOnce() {
        val now = System.currentTimeMillis()
        val expiresAt = now + membershipTtlMillis
        redis.withCommands { commands ->
            commands.zadd(membershipKeyBytes, expiresAt.toDouble(), podId.toByteArray(StandardCharsets.UTF_8))
            commands.zremrangebyscore(membershipKeyBytes, Double.NEGATIVE_INFINITY, now.toDouble())
        }
    }

    private suspend fun refreshOnce() {
        val now = System.currentTimeMillis()
        val pods = redis.withCommands { commands ->
            commands.zremrangebyscore(membershipKeyBytes, Double.NEGATIVE_INFINITY, now.toDouble())
            commands.zrangebyscore(
                membershipKeyBytes,
                now.toDouble(),
                Double.POSITIVE_INFINITY,
            )
        }.map { String(it, StandardCharsets.UTF_8) }
            .distinct()
            .sorted()

        allocator.updatePods(pods)
        lastHealthyPods.set(pods)
        lastRefreshAtMillis.set(now)
        lastError.set(null)
        metrics.setRedisActivePods(pods.size.toLong())
        metrics.setRedisOwnedShards(allocator.ownedShards().size.toLong())
        metrics.setRedisMembershipRefreshAgeMillis(0L)
    }
}

data class ReadinessStatus(
    val ok: Boolean,
    val message: String,
)
