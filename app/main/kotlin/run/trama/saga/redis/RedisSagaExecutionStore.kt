@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import io.lettuce.core.ScriptOutputType
import run.trama.saga.ExecutionPhase
import run.trama.saga.ExecutionState
import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionStore
import run.trama.saga.SleepEntry
import run.trama.saga.StepResult
import run.trama.saga.UuidAsStringSerializer
import run.trama.saga.WaitingInfo
import run.trama.saga.store.SagaRepository
import java.time.Instant
import java.util.UUID
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory

/**
 * Generous TTL for join-barrier keys (:expected/:arrived) — unlike :waiting/:sleep, there is
 * no known deadline to size the TTL against at registration time (branches may sleep for hours
 * or wait on long async callbacks). If it expires before all branches arrive, markChildArrived
 * falls back to the durable Postgres counter — correctness holds either way, this only affects
 * whether the fast path is used.
 */
private const val JOIN_BARRIER_TTL_SECONDS = 86_400L

class RedisSagaExecutionStore(
    private val redis: RedisCommandsProvider,
    private val repository: SagaRepository,
    private val keyspace: RedisShardKeyspace,
    private val ttlSeconds: Long = 600,
) : SagaExecutionStore {
    private val logger = LoggerFactory.getLogger(RedisSagaExecutionStore::class.java)
    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Atomically reads and deletes a key in a single round-trip.
     * Equivalent to Redis 6.2+ GETDEL but works on all versions via Lua.
     */
    private val getDelScript = """
        local v = redis.call('GET', KEYS[1])
        if v then redis.call('DEL', KEYS[1]) end
        return v
    """.trimIndent()

    /**
     * Atomically LPUSH + EXPIRE for step result insertion.
     * Avoids two separate round-trips per step result.
     */
    private val lpushExpireScript = """
        redis.call('LPUSH', KEYS[1], ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return 1
    """.trimIndent()

    /**
     * Idempotently marks ARGV[1] (a child execution id) as arrived in the SET at KEYS[1], and
     * atomically reads the resulting set size plus the expected count at KEYS[2] — all in one
     * round trip so "did I just add it" and "how many have arrived" can never race against a
     * concurrent sibling doing the same. SADD is a no-op if the member is already present, which
     * is exactly what makes this safe to call twice for the same child (a redelivered branch).
     * The EXPIRE lives in the same script (not a follow-up call) so a crash between the two can
     * never leave the arrived-set key with no TTL at all — same reasoning as [lpushExpireScript].
     * Returns "added:size:expected" (expected is empty when the key has expired/was evicted).
     */
    private val markArrivedScript = """
        local added = redis.call('SADD', KEYS[1], ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        local size = redis.call('SCARD', KEYS[1])
        local expected = redis.call('GET', KEYS[2])
        if not expected then expected = '' end
        return added .. ':' .. size .. ':' .. expected
    """.trimIndent()

    // SHA1 digests for pre-loaded scripts. Populated by [loadScripts].
    private var getDelScriptSha: String? = null
    private var lpushExpireScriptSha: String? = null
    private var markArrivedScriptSha: String? = null

    /** Loads Lua scripts into Redis at startup. Call once before processing begins. */
    suspend fun loadScripts() {
        redis.withCommands { commands ->
            getDelScriptSha = commands.scriptLoad(getDelScript.toByteArray())
            lpushExpireScriptSha = commands.scriptLoad(lpushExpireScript.toByteArray())
            markArrivedScriptSha = commands.scriptLoad(markArrivedScript.toByteArray())
        }
    }

    private suspend fun atomicGetDel(key: ByteArray): ByteArray? {
        return redis.withCommands { commands ->
            val sha = getDelScriptSha
            if (sha != null) {
                commands.evalsha<ByteArray>(sha, ScriptOutputType.VALUE, arrayOf(key))
            } else {
                commands.eval<ByteArray>(getDelScript.toByteArray(), ScriptOutputType.VALUE, arrayOf(key))
            }
        }
    }

    private suspend fun atomicMarkArrived(arrivedKey: ByteArray, expectedKey: ByteArray, childId: ByteArray, ttlSeconds: ByteArray): ByteArray? {
        return redis.withCommands { commands ->
            val sha = markArrivedScriptSha
            if (sha != null) {
                commands.evalsha<ByteArray>(sha, ScriptOutputType.VALUE, arrayOf(arrivedKey, expectedKey), childId, ttlSeconds)
            } else {
                commands.eval<ByteArray>(markArrivedScript.toByteArray(), ScriptOutputType.VALUE, arrayOf(arrivedKey, expectedKey), childId, ttlSeconds)
            }
        }
    }

    override suspend fun upsertStart(execution: SagaExecution) {
        val meta = RedisExecutionMeta(
            id = execution.id,
            name = execution.definition.name,
            version = execution.definition.version,
            definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition),
            startedAt = execution.startedAt,
            status = "IN_PROGRESS",
            failureDescription = null,
            callbackWarning = null,
            lastFailedStepIndex = null,
            lastFailedPhase = null,
        )
        writeMeta(meta)
    }

    override suspend fun updateFinal(executionId: UUID, status: String, failureDescription: String?) {
        val existing = readMeta(executionId)
        val meta = existing?.copy(
            status = status,
            failureDescription = failureDescription ?: existing.failureDescription,
        )
        if (meta == null) {
            logger.warn("missing redis meta on finalization for sagaId={}", executionId)
            repository.updateExecutionFinal(executionId, status, failureDescription)
            return
        }

        // Update Redis meta in one command (replaces SET + EXPIRE)
        writeMeta(meta)

        val steps = readSteps(meta.id)

        // All DB writes share a single connection — one HikariCP checkout for the whole finalization
        repository.finalizeExecution(
            id = meta.id,
            name = meta.name,
            version = meta.version,
            definitionJson = meta.definitionJson,
            startedAt = meta.startedAt,
            status = status,
            failureDescription = meta.failureDescription,
            lastFailedStepIndex = meta.lastFailedStepIndex,
            lastFailedPhase = meta.lastFailedPhase?.let { ExecutionPhase.valueOf(it) },
            callbackWarning = meta.callbackWarning,
            steps = steps,
        )

        deleteKeys(meta.id)
    }

    override suspend fun updateFailure(
        executionId: UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) {
        val current = readMeta(executionId)
        if (current == null) {
            logger.warn("missing redis meta on failure update for sagaId={}", executionId)
            return
        }
        writeMeta(
            current.copy(
                failureDescription = failureDescription,
                lastFailedStepIndex = failedStepIndex,
                lastFailedPhase = failedPhase?.name,
            )
        )
    }

    override suspend fun updateCallbackWarning(executionId: UUID, warning: String) {
        val current = readMeta(executionId)
        if (current == null) {
            logger.warn("missing redis meta on callback warning for sagaId={}", executionId)
            return
        }
        writeMeta(current.copy(callbackWarning = warning))
    }

    override suspend fun insertStepCalls(calls: List<run.trama.saga.StepCallEntry>) =
        repository.insertStepCalls(calls)

    override suspend fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepName: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: Instant?,
    ) {
        val entry = RedisStepEntry(
            stepIndex = stepIdx,
            stepName = stepName,
            phase = phase.name,
            statusCode = statusCode,
            success = success,
            responseBody = responseBody,
            startedAt = startedAt,
            createdAt = Instant.now(),
            stepStartedAt = stepStartedAt ?: Instant.EPOCH,
        )

        val key = stepsKey(sagaId).toByteArray()
        val payload = json.encodeToString(RedisStepEntry.serializer(), entry).toByteArray()
        redis.withCommands { commands ->
            val sha = lpushExpireScriptSha
            if (sha != null) {
                commands.evalsha<Long>(
                    sha,
                    ScriptOutputType.INTEGER,
                    arrayOf(key),
                    payload,
                    ttlSeconds.toString().toByteArray(),
                )
            } else {
                commands.lpush(key, payload)
                commands.expire(key, ttlSeconds)
            }
        }
        touchMetaTtl(sagaId)
    }

    override suspend fun loadStepResults(sagaId: UUID): List<StepResult> {
        val steps = readSteps(sagaId)
        if (steps.isEmpty()) return emptyList()

        val latestByIndex = linkedMapOf<Int, StepResult>()
        val sorted = steps.sortedWith(compareBy({ it.stepIndex }, { it.createdAt }))
        for (step in sorted) {
            val index = step.stepIndex
            val body = toJsonElement(step.responseBody)
            val current = latestByIndex[index] ?: StepResult(index = index, name = step.stepName, upBody = null, downBody = null)
            latestByIndex[index] = when (step.phase) {
                ExecutionPhase.UP.name   -> current.copy(upBody = body)
                ExecutionPhase.DOWN.name -> current.copy(downBody = body)
                else -> current
            }
        }
        return latestByIndex.values.toList()
    }

    private suspend fun readSteps(sagaId: UUID): List<RedisStepEntry> {
        val key = stepsKey(sagaId)
        return redis.withCommands { commands ->
            val raw = commands.lrange(key.toByteArray(), 0, -1)
            raw.mapNotNull { bytes ->
                runCatching {
                    json.decodeFromString(RedisStepEntry.serializer(), bytes.toString(Charsets.UTF_8))
                }.getOrNull()
            }
        }
    }

    private suspend fun writeMeta(meta: RedisExecutionMeta) {
        val key = metaKey(meta.id)
        val payload = json.encodeToString(RedisExecutionMeta.serializer(), meta).toByteArray()
        redis.withCommands { commands ->
            commands.setex(key.toByteArray(), ttlSeconds, payload)
        }
    }

    private suspend fun readMeta(executionId: UUID): RedisExecutionMeta? {
        val key = metaKey(executionId)
        return redis.withCommands { commands ->
            val raw = commands.get(key.toByteArray()) ?: return@withCommands null
            runCatching {
                json.decodeFromString(RedisExecutionMeta.serializer(), raw.toString(Charsets.UTF_8))
            }.getOrNull()
        }
    }

    private suspend fun touchMetaTtl(executionId: UUID) {
        val key = metaKey(executionId)
        redis.withCommands { commands ->
            commands.expire(key.toByteArray(), ttlSeconds)
        }
    }

    override suspend fun saveWaiting(execution: SagaExecution, signature: String) {
        val state = execution.state as? ExecutionState.WaitingCallback ?: return
        val executionJson = json.encodeToString(SagaExecution.serializer(), execution)
        val entry = RedisWaitingEntry(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = executionJson,
        )
        val key = waitingKey(execution.id).toByteArray()
        val value = json.encodeToString(RedisWaitingEntry.serializer(), entry).toByteArray()
        val ttl = (state.deadlineAt.epochSecond - Instant.now().epochSecond + 120).coerceAtLeast(60)
        redis.withCommands { commands ->
            commands.set(key, value)
            commands.expire(key, ttl)
        }

        // Write to Postgres so the status endpoint can surface WAITING_CALLBACK
        // and the callback timeout scanner can find timed-out executions.
        val definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition)
        repository.upsertExecutionRecord(execution.id, execution.definition.name, execution.definition.version, definitionJson, execution.startedAt)
        repository.saveWaitingState(
            executionId = execution.id,
            nodeId = state.nodeId,
            attempt = state.attempt,
            nonce = state.nonce,
            signature = signature,
            expiresAt = state.deadlineAt,
            executionJson = executionJson,
        )
    }

    override suspend fun consumeWaiting(executionId: UUID): WaitingInfo? {
        val key = waitingKey(executionId).toByteArray()
        val raw = atomicGetDel(key) ?: return null
        return runCatching {
            val entry = json.decodeFromString(RedisWaitingEntry.serializer(), raw.toString(Charsets.UTF_8))
            val execution = json.decodeFromString(SagaExecution.serializer(), entry.executionJson)
            WaitingInfo(
                nodeId = entry.nodeId,
                attempt = entry.attempt,
                nonce = entry.nonce,
                signature = entry.signature,
                expiresAt = entry.expiresAt,
                execution = execution,
            )
        }.getOrNull()
    }

    override suspend fun claimNonce(nonce: String, ttlSeconds: Long): Boolean {
        val key = "saga:nonce:$nonce".toByteArray()
        val value = "1".toByteArray()
        return redis.withCommands { commands ->
            commands.setNx(key, value, ttlSeconds)
        }
    }

    override suspend fun saveSleeping(execution: SagaExecution, wakeAt: Instant) {
        val state = execution.state as? ExecutionState.Sleeping ?: return
        val entry = RedisSleepEntry(
            wakeAt = wakeAt,
            executionJson = json.encodeToString(SagaExecution.serializer(), execution),
        )
        val key = sleepKey(execution.id).toByteArray()
        val value = json.encodeToString(RedisSleepEntry.serializer(), entry).toByteArray()
        // TTL: seconds until wakeAt + 2-hour buffer so the key outlives any re-enqueue chunks
        val ttl = (wakeAt.epochSecond - Instant.now().epochSecond + 7200).coerceAtLeast(120)
        redis.withCommands { commands ->
            commands.set(key, value)
            commands.expire(key, ttl)
        }
        // Update Postgres so the status API surfaces SLEEPING
        repository.updateStatus(execution.id, "SLEEPING")
    }

    override suspend fun peekSleeping(executionId: UUID): SleepEntry? {
        val key = sleepKey(executionId).toByteArray()
        val raw = redis.withCommands { commands -> commands.get(key) } ?: return null
        return parseSleepEntry(raw)
    }

    override suspend fun consumeSleeping(executionId: UUID): SleepEntry? {
        val key = sleepKey(executionId).toByteArray()
        val raw = atomicGetDel(key) ?: return null
        return parseSleepEntry(raw)
    }

    override suspend fun updateStatus(executionId: UUID, status: String) =
        repository.updateStatus(executionId, status)

    // ── Split / join ───────────────────────────────────────────────────────────
    // The arrival counter has a Redis fast path — a SET of arrived child ids (SADD), not a
    // raw INCR: SADD is idempotent by construction (adding the same member twice is a no-op),
    // which is exactly what a redelivered branch re-finalizing needs. registerJoinBarrier seeds
    // the `:expected` count; markArrivedScript atomically SADDs + SCARDs + reads `:expected` in
    // one round trip via Lua, so "did I just add it" and "how many have arrived" never race.
    // Postgres (saga_join_barrier/saga_join_branch) is still written on every call as a durable
    // mirror — it's what JoinCompletionScanner reads, and it's the fallback decision-maker if
    // the Redis keys are ever lost (TTL/eviction/restart). Branch linkage (getJoinBranches) has
    // no hot path of its own — it's read exactly once, when the join fires — so it stays
    // Postgres-only.

    private fun joinExpectedKey(executionId: UUID, splitNodeId: String) = keyspace.joinExpectedKey(executionId, splitNodeId)
    private fun joinArrivedKey(executionId: UUID, splitNodeId: String) = keyspace.joinArrivedKey(executionId, splitNodeId)

    override suspend fun registerJoinBarrier(
        parentId: UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        joinNodeId: String,
        branches: List<run.trama.saga.JoinBranchLink>,
    ): Set<String> {
        val expectedKey = joinExpectedKey(parentId, splitNodeId).toByteArray()
        redis.withCommands { commands ->
            commands.set(expectedKey, branches.size.toString().toByteArray())
            commands.expire(expectedKey, JOIN_BARRIER_TTL_SECONDS)
        }
        return repository.registerJoinBarrier(parentId, parentStartedAt, splitNodeId, joinNodeId, branches)
    }

    override suspend fun markChildArrived(
        parentId: UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        childId: UUID,
    ): run.trama.saga.JoinArrival? {
        val arrivedKey = joinArrivedKey(parentId, splitNodeId).toByteArray()
        val expectedKey = joinExpectedKey(parentId, splitNodeId).toByteArray()
        val raw = atomicMarkArrived(arrivedKey, expectedKey, childId.toString().toByteArray(), JOIN_BARRIER_TTL_SECONDS.toString().toByteArray())

        // Always mirror into Postgres — durable record for the backstop scanner and for
        // getJoinBranches; used as the decision-maker only if Redis lost the expected count.
        val postgresArrival = repository.markChildArrived(parentId, parentStartedAt, splitNodeId, childId)

        val parts = raw?.toString(Charsets.UTF_8)?.split(":")
        if (parts == null || parts.size != 3 || parts[2].isEmpty()) return postgresArrival
        val added = parts[0].toIntOrNull() ?: return postgresArrival
        val size = parts[1].toIntOrNull() ?: return postgresArrival
        val expected = parts[2].toIntOrNull() ?: return postgresArrival
        return run.trama.saga.JoinArrival(arrived = size, expected = expected, newlyMarked = added == 1)
    }

    override suspend fun getJoinBranches(
        parentId: UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
    ): List<run.trama.saga.JoinBranchLink> = repository.getJoinBranches(parentId, parentStartedAt, splitNodeId)

    override suspend fun saveWaitingJoin(execution: SagaExecution) {
        // Postgres-only: consumeWaitingJoin below no longer gives Redis an independent vote
        // (that was the round-2 fix for two stores each being able to declare a winner), so
        // writing a Redis mirror here would just be a wasted round trip that's never read back.
        val state = execution.state as? ExecutionState.WaitingJoin ?: return
        val executionJson = json.encodeToString(SagaExecution.serializer(), execution)

        // upsertStart only writes to Redis (the hot-path store), never Postgres — so under the
        // default REDIS store backend, the parent has no saga_execution row yet at this point.
        // saveWaitingJoinState below is a plain UPDATE ... WHERE id = ?, which would silently
        // affect zero rows without this — permanently losing the join pointer with no error,
        // no trace in the status API, and no way for JoinCompletionScanner to ever find it.
        // Same defensive upsert saveWaiting (the WaitingCallback sibling) already does above.
        val definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition)
        repository.upsertExecutionRecord(execution.id, execution.definition.name, execution.definition.version, definitionJson, execution.startedAt)
        repository.saveWaitingJoinState(
            executionId = execution.id,
            splitNodeId = state.splitNodeId,
            joinNodeId = state.joinNodeId,
            executionJson = executionJson,
        )
    }

    override suspend fun consumeWaitingJoin(executionId: UUID): SagaExecution? {
        // Postgres is the SOLE decision-maker here, unlike markChildArrived's per-branch hot
        // path: this is called once per split (by whichever branch wins the barrier, or by the
        // backstop scanner), not once per branch, so there is no meaningful load to save by
        // giving Redis an independent vote. Two independent atomic ops (one per store) each
        // able to return non-null previously let two concurrent callers (the winning branch and
        // the backstop scanner) both "win" from different stores. A single UPDATE ... RETURNING
        // is naturally serialized per row, so concurrent callers can never both succeed. There is
        // no Redis mirror to clean up here — saveWaitingJoin no longer writes one (see there).
        return repository.consumeWaitingJoinState(executionId)
    }

    override suspend fun getChildStatus(executionId: UUID): run.trama.saga.ChildExecutionStatus? =
        repository.getChildStatus(executionId)

    override suspend fun getChildStatuses(executionIds: List<UUID>): Map<UUID, run.trama.saga.ChildExecutionStatus> =
        repository.getChildStatuses(executionIds)

    private fun parseSleepEntry(raw: ByteArray): SleepEntry? =
        runCatching {
            val entry = json.decodeFromString(RedisSleepEntry.serializer(), raw.toString(Charsets.UTF_8))
            val execution = json.decodeFromString(SagaExecution.serializer(), entry.executionJson)
            SleepEntry(wakeAt = entry.wakeAt, execution = execution)
        }.getOrNull()

    private fun sleepKey(executionId: UUID): String = keyspace.sleepKey(executionId)

    private suspend fun deleteKeys(executionId: UUID) {
        val meta = metaKey(executionId).toByteArray()
        val steps = stepsKey(executionId).toByteArray()
        redis.withCommands { commands ->
            commands.del(meta, steps)
        }
    }

    private fun metaKey(executionId: UUID): String = keyspace.executionMetaKey(executionId)
    private fun stepsKey(executionId: UUID): String = keyspace.executionStepsKey(executionId)
    private fun waitingKey(executionId: UUID): String = keyspace.waitingKey(executionId)

    private fun toJsonElement(raw: String?): JsonElement? {
        if (raw == null) return null
        return try {
            json.parseToJsonElement(raw)
        } catch (_: Exception) {
            JsonPrimitive(raw)
        }.let { element -> if (element is JsonNull) null else element }
    }
}

@Serializable
data class RedisExecutionMeta(
    @Serializable(with = UuidAsStringSerializer::class)
    val id: UUID,
    val name: String,
    val version: String,
    val definitionJson: String,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    val status: String,
    val failureDescription: String?,
    val callbackWarning: String?,
    val lastFailedStepIndex: Int?,
    val lastFailedPhase: String?,
)

@Serializable
data class RedisStepEntry(
    val stepIndex: Int,
    val stepName: String,
    val phase: String,
    val statusCode: Int?,
    val success: Boolean,
    val responseBody: String?,
    @Serializable(with = InstantAsStringSerializer::class)
    val startedAt: Instant,
    @Serializable(with = InstantAsStringSerializer::class)
    val createdAt: Instant,
    @Serializable(with = InstantAsStringSerializer::class)
    val stepStartedAt: Instant = Instant.EPOCH,
)

@Serializable
data class RedisWaitingEntry(
    @Serializable(with = UuidAsStringSerializer::class)
    val executionId: UUID,
    val nodeId: String,
    val attempt: Int,
    val nonce: String,
    val signature: String,
    @Serializable(with = InstantAsStringSerializer::class)
    val expiresAt: Instant,
    /** Full [SagaExecution] JSON (with WaitingCallback state) for re-enqueueing on valid callback. */
    val executionJson: String,
)

@Serializable
data class RedisSleepEntry(
    @Serializable(with = InstantAsStringSerializer::class)
    val wakeAt: Instant,
    /** Full [SagaExecution] JSON (with Sleeping state) for re-enqueueing on wake. */
    val executionJson: String,
)

