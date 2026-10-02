package run.trama.saga.store

import run.trama.saga.ChildExecutionStatus
import run.trama.saga.ExecutionState
import run.trama.saga.Parking
import run.trama.saga.PayloadValue
import run.trama.saga.PersistedCheckpoint
import run.trama.saga.StepRecord
import run.trama.saga.UuidAsStringSerializer
import run.trama.saga.ExecutionPhase
import run.trama.saga.JoinArrival
import run.trama.saga.JoinBranchLink
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaExecution
import run.trama.saga.SleepEntry
import run.trama.saga.StepCallEntry
import run.trama.saga.WaitingInfo
import run.trama.runtime.CallbackTimeoutRepository
import run.trama.runtime.JoinBarrierRepository
import run.trama.runtime.StalledExecutionRepository
import run.trama.jooq.Tables.SAGA_DEFINITION
import run.trama.jooq.Tables.SAGA_EXECUTION
import run.trama.jooq.Tables.SAGA_STEP_CALL
import run.trama.jooq.Tables.SAGA_STEP_RESULT
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.jooq.JSONB
import org.jooq.impl.DSL
import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.StepResult
import run.trama.saga.redis.RedisStepEntry

private fun Instant.toOffset(): OffsetDateTime = atOffset(ZoneOffset.UTC)
private fun OffsetDateTime?.toInstant(): Instant = this?.toInstant() ?: Instant.EPOCH

private val rowJson = Json { ignoreUnknownKeys = true }

/**
 * The definition persisted in `saga_execution.definition`: the real v2 graph when the execution
 * has one (its v1 `definition` is only a name/version stub), otherwise the v1 definition.
 */
fun SagaExecution.persistedDefinitionJson(): String =
    definitionV2?.let { rowJson.encodeToString(SagaDefinitionV2.serializer(), it) }
        ?: rowJson.encodeToString(SagaDefinition.serializer(), definition)

/** The run payload persisted in `saga_execution.payload`; null when empty (stored as SQL NULL). */
fun SagaExecution.persistedPayloadJson(): String? =
    payload.takeIf { it.isNotEmpty() }?.let { p -> JsonObject(p.mapValues { it.value.value }).toString() }

class SagaRepository(
    private val db: DatabaseClient,
    definitionCacheMaxSize: Int = 1000,
    /**
     * How long a cached definition is served without re-reading Postgres. The cache is per pod and
     * only invalidated locally, so this bounds how long another pod may keep serving (and running)
     * a definition that was deleted elsewhere. 0 disables caching.
     */
    definitionCacheTtlMillis: Long = 5_000,
) : CallbackTimeoutRepository, JoinBarrierRepository, StalledExecutionRepository {
    private class CachedDefinition(val record: SagaDefinitionRecord, val cachedAtNanos: Long)

    private val definitionCacheTtlNanos = definitionCacheTtlMillis * 1_000_000
    private val definitionCache: MutableMap<UUID, CachedDefinition> =
        java.util.Collections.synchronizedMap(
            object : java.util.LinkedHashMap<UUID, CachedDefinition>(
                minOf(definitionCacheMaxSize, 16), 0.75f, true
            ) {
                override fun removeEldestEntry(eldest: Map.Entry<UUID, CachedDefinition>) =
                    size > definitionCacheMaxSize
            }
        )
    private val definitionNameVersionCache: MutableMap<String, UUID> =
        java.util.Collections.synchronizedMap(
            object : java.util.LinkedHashMap<String, UUID>(
                minOf(definitionCacheMaxSize, 16), 0.75f, true
            ) {
                override fun removeEldestEntry(eldest: Map.Entry<String, UUID>) =
                    size > definitionCacheMaxSize
            }
        )
    private val json = Json { ignoreUnknownKeys = true }

    /** The single way an execution's row is created or refreshed from a live [SagaExecution]. */
    suspend fun upsertExecutionStart(execution: SagaExecution) {
        upsertExecutionRecord(
            id = execution.id,
            name = execution.definition.name,
            version = execution.definition.version,
            definitionJson = execution.persistedDefinitionJson(),
            startedAt = execution.startedAt,
            payloadJson = execution.persistedPayloadJson(),
        )
    }

    /**
     * Ensures a row exists in `saga_execution` for the given [id]/[startedAt] combination.
     * On conflict, preserves any existing definition/payload and resets status to `IN_PROGRESS`,
     * unless the execution already finished.
     */
    suspend fun upsertExecutionRecord(
        id: UUID,
        name: String,
        version: String,
        definitionJson: String,
        startedAt: Instant,
        payloadJson: String? = null,
    ) {
        val definitionJsonb = JSONB.valueOf(definitionJson)
        val payloadJsonb = payloadJson?.let { JSONB.valueOf(it) }
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
            dsl.insertInto(SAGA_EXECUTION)
                .columns(
                    SAGA_EXECUTION.ID,
                    SAGA_EXECUTION.NAME,
                    SAGA_EXECUTION.VERSION,
                    SAGA_EXECUTION.DEFINITION,
                    SAGA_EXECUTION.STATUS,
                    SAGA_EXECUTION.STARTED_AT,
                    SAGA_EXECUTION.UPDATED_AT,
                    SAGA_EXECUTION.PAYLOAD,
                )
                .values(
                    id,
                    name,
                    version,
                    definitionJsonb,
                    "IN_PROGRESS",
                    startedAt.toOffset(),
                    now,
                    payloadJsonb,
                )
                .onConflict(SAGA_EXECUTION.ID, SAGA_EXECUTION.STARTED_AT)
                .doUpdate()
                .set(SAGA_EXECUTION.STATUS, "IN_PROGRESS")
                .set(SAGA_EXECUTION.DEFINITION,
                    DSL.coalesce(SAGA_EXECUTION.DEFINITION, definitionJsonb))
                .set(SAGA_EXECUTION.PAYLOAD, DSL.coalesce(SAGA_EXECUTION.PAYLOAD, payloadJsonb))
                .set(SAGA_EXECUTION.UPDATED_AT, now)
                // A finished execution is never reopened here; only the retry endpoint does that.
                .where(SAGA_EXECUTION.STATUS.notIn(PersistedCheckpoint.TERMINAL_STATUSES))
                .execute()
        }
    }

    suspend fun updateExecutionFinal(
        executionId: UUID,
        status: String,
        failureDescription: String? = null,
    ) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.STATUS, status)
                .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, failureDescription)
                .set(SAGA_EXECUTION.COMPLETED_AT, now)
                .set(SAGA_EXECUTION.UPDATED_AT, now)
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun updateFailureDescription(
        executionId: UUID,
        failureDescription: String,
        failedStepIndex: Int?,
        failedPhase: ExecutionPhase?,
    ) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, failureDescription)
                .set(SAGA_EXECUTION.LAST_FAILED_STEP_INDEX, failedStepIndex)
                .set(SAGA_EXECUTION.LAST_FAILED_PHASE, failedPhase?.name)
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun updateCallbackWarning(executionId: UUID, warning: String) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.CALLBACK_WARNING, warning)
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun updateStatus(executionId: UUID, status: String) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.STATUS, status)
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun insertStepResult(
        sagaId: UUID,
        startedAt: Instant,
        stepIdx: Int,
        stepNameValue: String,
        phase: ExecutionPhase,
        statusCode: Int?,
        success: Boolean,
        responseBody: String?,
        stepStartedAt: Instant? = null,
    ) {
        val bodyJson = responseBody?.let { toJsonb(it) }
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.insertInto(SAGA_STEP_RESULT)
                .columns(
                    SAGA_STEP_RESULT.SAGA_ID,
                    SAGA_STEP_RESULT.STEP_INDEX,
                    SAGA_STEP_RESULT.STEP_NAME,
                    SAGA_STEP_RESULT.PHASE,
                    SAGA_STEP_RESULT.STATUS_CODE,
                    SAGA_STEP_RESULT.SUCCESS,
                    SAGA_STEP_RESULT.RESPONSE_BODY,
                    SAGA_STEP_RESULT.STEP_STARTED_AT,
                    SAGA_STEP_RESULT.STARTED_AT,
                    SAGA_STEP_RESULT.CREATED_AT,
                )
                .values(
                    sagaId,
                    stepIdx,
                    stepNameValue,
                    phase.name,
                    statusCode,
                    success,
                    bodyJson,
                    stepStartedAt?.toOffset(),
                    startedAt.toOffset(),
                    Instant.now().toOffset(),
                )
                .execute()
        }
    }

    /** Hot path (once per execution slice): plain JDBC, no query rendering. */
    suspend fun loadStepResultsForTemplate(sagaId: UUID): List<StepResult> {
        return db.withConnection { connection ->
            val sql = """
                SELECT step_index, step_name, phase, response_body FROM saga_step_result
                WHERE saga_id = ? AND started_at >= ?
                ORDER BY step_index ASC, created_at ASC
            """.trimIndent()
            val latestByIndex = linkedMapOf<Int, StepResult>()
            connection.prepareStatement(sql).use { ps ->
                ps.setObject(1, sagaId)
                ps.setObject(2, cutoff())
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        val index = rs.getInt(1).takeUnless { rs.wasNull() } ?: continue
                        val name = rs.getString(2) ?: ""
                        val phase = rs.getString(3) ?: ExecutionPhase.UP.name
                        val body = rs.getString(4)?.let { parseJson(it) }
                        val current = latestByIndex[index] ?: StepResult(index = index, name = name, upBody = null, downBody = null)
                        latestByIndex[index] = when (phase) {
                            ExecutionPhase.UP.name -> current.copy(upBody = body)
                            ExecutionPhase.DOWN.name -> current.copy(downBody = body)
                            else -> current
                        }
                    }
                }
            }
            latestByIndex.values.toList()
        }
    }

    suspend fun getExecutionStatus(sagaId: UUID): SagaExecutionStatus? {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                SAGA_EXECUTION.ID,
                SAGA_EXECUTION.NAME,
                SAGA_EXECUTION.VERSION,
                SAGA_EXECUTION.DEFINITION,
                SAGA_EXECUTION.STATUS,
                SAGA_EXECUTION.FAILURE_DESCRIPTION,
                SAGA_EXECUTION.CALLBACK_WARNING,
                SAGA_EXECUTION.LAST_FAILED_STEP_INDEX,
                SAGA_EXECUTION.LAST_FAILED_PHASE,
                SAGA_EXECUTION.STARTED_AT,
                SAGA_EXECUTION.COMPLETED_AT,
                SAGA_EXECUTION.UPDATED_AT,
            )
                .from(SAGA_EXECUTION)
                .where(SAGA_EXECUTION.ID.eq(sagaId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_EXECUTION.STARTED_AT.desc())
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaExecutionStatus(
                id = record.get(SAGA_EXECUTION.ID) ?: sagaId,
                name = record.get(SAGA_EXECUTION.NAME) ?: "",
                version = record.get(SAGA_EXECUTION.VERSION) ?: "",
                definition = record.get(SAGA_EXECUTION.DEFINITION)?.data(),
                status = record.get(SAGA_EXECUTION.STATUS) ?: "UNKNOWN",
                failureDescription = record.get(SAGA_EXECUTION.FAILURE_DESCRIPTION),
                callbackWarning = record.get(SAGA_EXECUTION.CALLBACK_WARNING),
                lastFailedStepIndex = record.get(SAGA_EXECUTION.LAST_FAILED_STEP_INDEX),
                lastFailedPhase = record.get(SAGA_EXECUTION.LAST_FAILED_PHASE),
                startedAt = record.get(SAGA_EXECUTION.STARTED_AT).toInstant(),
                completedAt = record.get(SAGA_EXECUTION.COMPLETED_AT)?.toInstant(),
                updatedAt = record.get(SAGA_EXECUTION.UPDATED_AT).toInstant(),
            )
        }
    }

    suspend fun saveWaitingState(
        executionId: UUID,
        nodeId: String,
        attempt: Int,
        nonce: String,
        signature: String,
        expiresAt: Instant,
        executionJson: String,
    ) {
        val state = WaitingStateJson(
            nodeId = nodeId,
            attempt = attempt,
            nonce = nonce,
            signature = signature,
            expiresAt = expiresAt,
            executionJson = executionJson,
        )
        val waitingJson = JSONB.valueOf(json.encodeToString(WaitingStateJson.serializer(), state))
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.WAITING_STATE, waitingJson)
                .set(SAGA_EXECUTION.STATUS, "WAITING_CALLBACK")
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun consumeWaitingState(executionId: UUID): WaitingInfo? {
        return db.withConnection { connection ->
            // Atomically clear and return waiting_state in one round-trip. RETURNING evaluates
            // against the row's POST-update state, so a bare `SET waiting_state = NULL ...
            // RETURNING waiting_state` would always return NULL — the CTE below locks and
            // captures the value BEFORE the UPDATE's SET clause overwrites it. The FOR UPDATE
            // lock inside the CTE still makes this a single atomic statement: a second concurrent
            // caller blocks on the row lock, then (once the first commits) sees waiting_state
            // already NULL and legitimately matches zero rows.
            val sql = """
                WITH captured AS (
                    SELECT id, waiting_state FROM saga_execution
                    WHERE id = ? AND started_at >= ? AND waiting_state IS NOT NULL
                    FOR UPDATE
                )
                UPDATE saga_execution se
                SET waiting_state = NULL, updated_at = now()
                FROM captured c
                WHERE se.id = c.id
                RETURNING c.waiting_state
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps ->
                ps.setObject(1, executionId)
                ps.setObject(2, java.sql.Timestamp.from(cutoff().toInstant()))
            }.executeQuery()
            if (!rs.next()) return@withConnection null
            val rawJson = rs.getString("waiting_state") ?: return@withConnection null

            runCatching {
                val state = json.decodeFromString(WaitingStateJson.serializer(), rawJson)
                val execution = json.decodeFromString(run.trama.saga.SagaExecution.serializer(), state.executionJson)
                WaitingInfo(
                    nodeId = state.nodeId,
                    attempt = state.attempt,
                    nonce = state.nonce,
                    signature = state.signature,
                    expiresAt = state.expiresAt,
                    execution = execution,
                )
            }.getOrNull()
        }
    }

    // ── Split / join ───────────────────────────────────────────────────────────
    // saga_join_barrier / saga_join_branch are plain (non-partitioned, non-jOOQ) control
    // tables accessed via raw JDBC — deliberately kept out of jOOQ codegen so this feature
    // needs no schema regeneration; see db.changelog-master.xml changeset 006.

    /**
     * Returns the [JoinBranchLink.branchId]s newly registered by *this* call (i.e. rows that did
     * not already exist), via `INSERT ... ON CONFLICT DO NOTHING RETURNING` — the same idempotent
     * pattern as [markChildArrived]. A redelivered split step reconstructs the exact same
     * deterministic branch set, so a branch missing from the returned set here was already
     * registered (and, by construction, already enqueued) by an earlier attempt; the caller must
     * not enqueue it again. One round trip per branch rather than a single batch, since a batch
     * INSERT cannot report per-row RETURNING results — an acceptable cost given branches only
     * spawn once per split, not on every barrier arrival.
     */
    suspend fun registerJoinBarrier(
        parentId: UUID,
        parentStartedAt: Instant,
        splitNodeId: String,
        joinNodeId: String,
        branches: List<JoinBranchLink>,
    ): Set<String> {
        return db.withConnection { connection ->
            val now = java.sql.Timestamp.from(Instant.now())
            connection.prepareStatement(
                """
                INSERT INTO saga_join_barrier
                    (parent_id, parent_started_at, split_node_id, join_node_id, expected_count, arrived_count, created_at)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT (parent_id, parent_started_at, split_node_id) DO NOTHING
                """.trimIndent()
            ).use { ps ->
                ps.setObject(1, parentId)
                ps.setTimestamp(2, java.sql.Timestamp.from(parentStartedAt))
                ps.setString(3, splitNodeId)
                ps.setString(4, joinNodeId)
                ps.setInt(5, branches.size)
                ps.setTimestamp(6, now)
                ps.executeUpdate()
            }
            if (branches.isEmpty()) return@withConnection emptySet()
            val newlyRegistered = mutableSetOf<String>()
            connection.prepareStatement(
                """
                INSERT INTO saga_join_branch
                    (parent_id, parent_started_at, split_node_id, branch_id, child_id, child_started_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (parent_id, parent_started_at, split_node_id, branch_id) DO NOTHING
                RETURNING branch_id
                """.trimIndent()
            ).use { ps ->
                for (branch in branches) {
                    ps.setObject(1, parentId)
                    ps.setTimestamp(2, java.sql.Timestamp.from(parentStartedAt))
                    ps.setString(3, splitNodeId)
                    ps.setString(4, branch.branchId)
                    ps.setObject(5, branch.childId)
                    ps.setTimestamp(6, java.sql.Timestamp.from(branch.childStartedAt))
                    ps.setTimestamp(7, now)
                    val rs = ps.executeQuery()
                    if (rs.next()) newlyRegistered += rs.getString("branch_id")
                }
            }
            newlyRegistered
        }
    }

    /**
     * Idempotent per [childId]: the `arrived_at IS NULL` guard means a redelivered branch that
     * already marked its own arrival contributes 0 to the barrier's counter on any later call —
     * `newly_marked` tells the caller whether *this* call was the one that actually transitioned
     * the row, so only a genuine first-arrival can ever be treated as the barrier's winner.
     * Single round trip, no lock: the two UPDATEs are each atomic per-row, chained by one CTE.
     */
    suspend fun markChildArrived(parentId: UUID, parentStartedAt: Instant, splitNodeId: String, childId: UUID): JoinArrival? {
        return db.withConnection { connection ->
            val sql = """
                WITH marked AS (
                    UPDATE saga_join_branch
                    SET arrived_at = now()
                    WHERE parent_id = ? AND parent_started_at = ? AND split_node_id = ?
                      AND child_id = ? AND arrived_at IS NULL
                    RETURNING 1
                ),
                bumped AS (
                    UPDATE saga_join_barrier b
                    SET arrived_count = b.arrived_count + (SELECT count(*) FROM marked)
                    WHERE b.parent_id = ? AND b.parent_started_at = ? AND b.split_node_id = ?
                    RETURNING b.arrived_count, b.expected_count
                )
                SELECT bumped.arrived_count, bumped.expected_count, (SELECT count(*) FROM marked) > 0 AS newly_marked
                FROM bumped
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps ->
                ps.setObject(1, parentId)
                ps.setTimestamp(2, java.sql.Timestamp.from(parentStartedAt))
                ps.setString(3, splitNodeId)
                ps.setObject(4, childId)
                ps.setObject(5, parentId)
                ps.setTimestamp(6, java.sql.Timestamp.from(parentStartedAt))
                ps.setString(7, splitNodeId)
            }.executeQuery()
            if (!rs.next()) return@withConnection null
            JoinArrival(
                arrived = rs.getInt("arrived_count"),
                expected = rs.getInt("expected_count"),
                newlyMarked = rs.getBoolean("newly_marked"),
            )
        }
    }

    suspend fun getJoinBranches(parentId: UUID, parentStartedAt: Instant, splitNodeId: String): List<JoinBranchLink> {
        return db.withConnection { connection ->
            val sql = """
                SELECT branch_id, child_id, child_started_at FROM saga_join_branch
                WHERE parent_id = ? AND parent_started_at = ? AND split_node_id = ?
                ORDER BY id ASC
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps ->
                ps.setObject(1, parentId)
                ps.setTimestamp(2, java.sql.Timestamp.from(parentStartedAt))
                ps.setString(3, splitNodeId)
            }.executeQuery()
            val links = mutableListOf<JoinBranchLink>()
            while (rs.next()) {
                links += JoinBranchLink(
                    branchId = rs.getString("branch_id"),
                    childId = UUID.fromString(rs.getString("child_id")),
                    childStartedAt = rs.getTimestamp("child_started_at").toInstant(),
                )
            }
            links
        }
    }

    // ── Sleep sentinel (POSTGRES store) ────────────────────────────────────────
    // Same parking slot as callbacks/joins (waiting_state), discriminated by status = 'SLEEPING'.
    // CallbackTimeoutScanner only looks at WAITING_CALLBACK rows, so it never sees these.

    suspend fun saveSleepingState(executionId: UUID, wakeAt: Instant, executionJson: String) {
        val stateJson = JSONB.valueOf(json.encodeToString(SleepStateJson.serializer(), SleepStateJson(wakeAt, executionJson)))
        db.withConnection { connection ->
            DSL.using(connection).update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.WAITING_STATE, stateJson)
                .set(SAGA_EXECUTION.STATUS, "SLEEPING")
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun peekSleepingState(executionId: UUID): SleepEntry? {
        val raw = db.withConnection { connection ->
            DSL.using(connection).select(SAGA_EXECUTION.WAITING_STATE)
                .from(SAGA_EXECUTION)
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .and(SAGA_EXECUTION.STATUS.eq("SLEEPING"))
                .fetchOne(SAGA_EXECUTION.WAITING_STATE)
                ?.data()
        }
        return raw?.let(::parseSleepEntry)
    }

    /** Atomically clears and returns the sleep sentinel; only one concurrent caller gets it. */
    suspend fun consumeSleepingState(executionId: UUID): SleepEntry? {
        val raw = db.withConnection { connection ->
            // Same lock-then-clear CTE as consumeWaitingState (RETURNING sees the post-update row,
            // so the value must be captured before SET), guarded on status = 'SLEEPING'.
            val sql = """
                WITH captured AS (
                    SELECT id, waiting_state FROM saga_execution
                    WHERE id = ? AND started_at >= ? AND status = 'SLEEPING' AND waiting_state IS NOT NULL
                    FOR UPDATE
                )
                UPDATE saga_execution se
                SET waiting_state = NULL, updated_at = now()
                FROM captured c
                WHERE se.id = c.id
                RETURNING c.waiting_state
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setObject(1, executionId)
                ps.setObject(2, java.sql.Timestamp.from(cutoff().toInstant()))
                ps.executeQuery().use { rs -> if (rs.next()) rs.getString("waiting_state") else null }
            }
        }
        return raw?.let(::parseSleepEntry)
    }

    private fun parseSleepEntry(raw: String): SleepEntry? = runCatching {
        val state = json.decodeFromString(SleepStateJson.serializer(), raw)
        SleepEntry(state.wakeAt, json.decodeFromString(SagaExecution.serializer(), state.executionJson))
    }.getOrNull()

    suspend fun saveWaitingJoinState(
        executionId: UUID,
        splitNodeId: String,
        joinNodeId: String,
        executionJson: String,
    ) {
        val state = WaitingJoinStateJson(splitNodeId = splitNodeId, joinNodeId = joinNodeId, executionJson = executionJson)
        val waitingJson = JSONB.valueOf(json.encodeToString(WaitingJoinStateJson.serializer(), state))
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.WAITING_STATE, waitingJson)
                .set(SAGA_EXECUTION.STATUS, "WAITING_JOIN")
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
        }
    }

    suspend fun consumeWaitingJoinState(executionId: UUID): SagaExecution? {
        return db.withConnection { connection ->
            // Also flips status away from WAITING_JOIN here (not just waiting_state to NULL) so
            // findStalledJoinBarriers can no longer re-match this row in the window before the
            // re-enqueued parent is actually dequeued and checkpointed.
            //
            // RETURNING evaluates against the row's POST-update state, so a bare
            // `SET waiting_state = NULL ... RETURNING waiting_state` always returns NULL —
            // confirmed live against r2d2 (2026-09-21): the row's status correctly flipped to
            // IN_PROGRESS but this method still returned null every time, silently breaking every
            // split/join resume. The CTE below locks and captures the value BEFORE the UPDATE's
            // SET clause overwrites it; the FOR UPDATE lock inside the CTE keeps this one atomic
            // statement, so only a single concurrent caller can ever see a non-null result (a
            // second caller blocks on the row lock, then sees waiting_state already NULL once the
            // first commits, and legitimately matches zero rows — same guarantee as before).
            val sql = """
                WITH captured AS (
                    SELECT id, waiting_state FROM saga_execution
                    WHERE id = ? AND started_at >= ? AND waiting_state IS NOT NULL
                    FOR UPDATE
                )
                UPDATE saga_execution se
                SET waiting_state = NULL, status = 'IN_PROGRESS', updated_at = now()
                FROM captured c
                WHERE se.id = c.id
                RETURNING c.waiting_state
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps ->
                ps.setObject(1, executionId)
                ps.setObject(2, java.sql.Timestamp.from(cutoff().toInstant()))
            }.executeQuery()
            if (!rs.next()) return@withConnection null
            val rawJson = rs.getString("waiting_state") ?: return@withConnection null
            runCatching {
                val state = json.decodeFromString(WaitingJoinStateJson.serializer(), rawJson)
                json.decodeFromString(SagaExecution.serializer(), state.executionJson)
            }.getOrNull()
        }
    }

    override suspend fun findStalledJoinBarriers(limit: Int): List<UUID> {
        return db.withConnection { connection ->
            val sql = """
                SELECT b.parent_id FROM saga_join_barrier b
                JOIN saga_execution e ON e.id = b.parent_id AND e.started_at = b.parent_started_at
                WHERE e.status = 'WAITING_JOIN' AND (
                    b.arrived_count >= b.expected_count
                    -- Every branch finished, even if an arrival was never recorded (the branch's
                    -- process died, or its Postgres call failed, between finishing and arriving).
                    OR (SELECT count(*) FROM saga_join_branch jb
                        JOIN saga_execution c ON c.id = jb.child_id AND c.started_at = jb.child_started_at
                        WHERE jb.parent_id = b.parent_id AND jb.parent_started_at = b.parent_started_at
                          AND jb.split_node_id = b.split_node_id
                          AND c.status IN ('SUCCEEDED', 'FAILED', 'CORRUPTED')) >= b.expected_count
                )
                LIMIT ?
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps -> ps.setInt(1, limit) }.executeQuery()
            val ids = mutableListOf<UUID>()
            while (rs.next()) {
                ids += UUID.fromString(rs.getString("parent_id"))
            }
            ids
        }
    }

    suspend fun getChildStatus(executionId: UUID): ChildExecutionStatus? {
        val status = getExecutionStatus(executionId) ?: return null
        val lastResult = db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.select(SAGA_STEP_RESULT.RESPONSE_BODY)
                .from(SAGA_STEP_RESULT)
                .where(SAGA_STEP_RESULT.SAGA_ID.eq(executionId))
                .and(SAGA_STEP_RESULT.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_STEP_RESULT.CREATED_AT.desc())
                .limit(1)
                .fetchOne()
                ?.get(SAGA_STEP_RESULT.RESPONSE_BODY)?.data()
        }
        return ChildExecutionStatus(
            status = status.status,
            failureDescription = status.failureDescription,
            lastResultJson = lastResult,
        )
    }

    /**
     * Batched form of [getChildStatus]: two queries total regardless of how many [executionIds]
     * are passed (one for status/failure, one for each child's last step result via
     * DISTINCT ON), instead of 2*N round trips from calling [getChildStatus] in a loop.
     */
    suspend fun getChildStatuses(executionIds: List<UUID>): Map<UUID, ChildExecutionStatus> {
        if (executionIds.isEmpty()) return emptyMap()
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val statusById = dsl.select(
                SAGA_EXECUTION.ID,
                SAGA_EXECUTION.STATUS,
                SAGA_EXECUTION.FAILURE_DESCRIPTION,
            )
                .from(SAGA_EXECUTION)
                .where(SAGA_EXECUTION.ID.`in`(executionIds))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .fetch()
                .associate { r -> r.get(SAGA_EXECUTION.ID)!! to (r.get(SAGA_EXECUTION.STATUS) to r.get(SAGA_EXECUTION.FAILURE_DESCRIPTION)) }

            val lastResultById = mutableMapOf<UUID, String?>()
            val sql = """
                SELECT DISTINCT ON (saga_id) saga_id, response_body
                FROM saga_step_result
                WHERE saga_id = ANY(?) AND started_at >= ?
                ORDER BY saga_id, created_at DESC
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setArray(1, connection.createArrayOf("uuid", executionIds.toTypedArray()))
                ps.setTimestamp(2, java.sql.Timestamp.from(cutoff().toInstant()))
                val rs = ps.executeQuery()
                while (rs.next()) {
                    lastResultById[UUID.fromString(rs.getString("saga_id"))] = rs.getString("response_body")
                }
            }

            statusById.mapValues { (id, pair) ->
                ChildExecutionStatus(
                    status = pair.first ?: "UNKNOWN",
                    failureDescription = pair.second,
                    lastResultJson = lastResultById[id],
                )
            }
        }
    }

    suspend fun getExecutionForRetry(sagaId: UUID): SagaExecutionRetryData? {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                SAGA_EXECUTION.ID,
                SAGA_EXECUTION.DEFINITION,
                SAGA_EXECUTION.LAST_FAILED_STEP_INDEX,
                SAGA_EXECUTION.LAST_FAILED_PHASE,
                SAGA_EXECUTION.STARTED_AT,
                SAGA_EXECUTION.STATUS,
                SAGA_EXECUTION.PAYLOAD,
            )
                .from(SAGA_EXECUTION)
                .where(SAGA_EXECUTION.ID.eq(sagaId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_EXECUTION.STARTED_AT.desc())
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaExecutionRetryData(
                id = record.get(SAGA_EXECUTION.ID) ?: sagaId,
                definitionJson = record.get(SAGA_EXECUTION.DEFINITION)?.data(),
                failedStepIndex = record.get(SAGA_EXECUTION.LAST_FAILED_STEP_INDEX),
                failedPhase = record.get(SAGA_EXECUTION.LAST_FAILED_PHASE),
                startedAt = record.get(SAGA_EXECUTION.STARTED_AT).toInstant(),
                status = record.get(SAGA_EXECUTION.STATUS) ?: "UNKNOWN",
                payloadJson = record.get(SAGA_EXECUTION.PAYLOAD)?.data(),
            )
        }
    }

    suspend fun insertDefinition(id: UUID, name: String, version: String, definitionJson: String): Boolean {
        val now = Instant.now().toOffset()
        val inserted = db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.insertInto(SAGA_DEFINITION)
                .columns(
                    SAGA_DEFINITION.ID,
                    SAGA_DEFINITION.NAME,
                    SAGA_DEFINITION.VERSION,
                    SAGA_DEFINITION.DEFINITION,
                    SAGA_DEFINITION.CREATED_AT,
                    SAGA_DEFINITION.UPDATED_AT,
                )
                .values(
                    id,
                    name,
                    version,
                    JSONB.valueOf(definitionJson),
                    now,
                    now,
                )
                .onConflictDoNothing()
                .execute()
        }
        if (inserted == 0) return false
        putDefinitionInCache(
            SagaDefinitionRecord(
                id = id,
                name = name,
                version = version,
                definitionJson = definitionJson,
                createdAt = Instant.now(),
                updatedAt = Instant.now(),
            )
        )
        return true
    }

    suspend fun getDefinition(id: UUID): SagaDefinitionRecord? {
        freshCachedDefinition(id)?.let { return it }
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                SAGA_DEFINITION.ID,
                SAGA_DEFINITION.NAME,
                SAGA_DEFINITION.VERSION,
                SAGA_DEFINITION.DEFINITION,
                SAGA_DEFINITION.CREATED_AT,
                SAGA_DEFINITION.UPDATED_AT,
            )
                .from(SAGA_DEFINITION)
                .where(SAGA_DEFINITION.ID.eq(id))
                .fetchOne()
                ?: return@withConnection null

            SagaDefinitionRecord(
                id = record.get(SAGA_DEFINITION.ID) ?: id,
                name = record.get(SAGA_DEFINITION.NAME) ?: "",
                version = record.get(SAGA_DEFINITION.VERSION) ?: "",
                definitionJson = record.get(SAGA_DEFINITION.DEFINITION)?.data() ?: "",
                createdAt = record.get(SAGA_DEFINITION.CREATED_AT).toInstant(),
                updatedAt = record.get(SAGA_DEFINITION.UPDATED_AT).toInstant(),
            ).also { putDefinitionInCache(it) }
        }
    }

    suspend fun getDefinitionByNameVersion(name: String, version: String): SagaDefinitionRecord? {
        val key = definitionNameVersionKey(name, version)
        definitionNameVersionCache[key]?.let { id ->
            freshCachedDefinition(id)?.let { return it }
            definitionNameVersionCache.remove(key, id)
        }

        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                SAGA_DEFINITION.ID,
                SAGA_DEFINITION.NAME,
                SAGA_DEFINITION.VERSION,
                SAGA_DEFINITION.DEFINITION,
                SAGA_DEFINITION.CREATED_AT,
                SAGA_DEFINITION.UPDATED_AT,
            )
                .from(SAGA_DEFINITION)
                .where(SAGA_DEFINITION.NAME.eq(name))
                .and(SAGA_DEFINITION.VERSION.eq(version))
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaDefinitionRecord(
                id = record.get(SAGA_DEFINITION.ID) ?: UUID.randomUUID(),
                name = record.get(SAGA_DEFINITION.NAME) ?: "",
                version = record.get(SAGA_DEFINITION.VERSION) ?: "",
                definitionJson = record.get(SAGA_DEFINITION.DEFINITION)?.data() ?: "",
                createdAt = record.get(SAGA_DEFINITION.CREATED_AT).toInstant(),
                updatedAt = record.get(SAGA_DEFINITION.UPDATED_AT).toInstant(),
            ).also { putDefinitionInCache(it) }
        }
    }

    suspend fun deleteDefinition(id: UUID): Boolean {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val deleted = dsl.deleteFrom(SAGA_DEFINITION)
                .where(SAGA_DEFINITION.ID.eq(id))
                .execute() > 0
            if (deleted) {
                val removed = definitionCache.remove(id)?.record
                if (removed != null) {
                    definitionNameVersionCache.remove(
                        definitionNameVersionKey(removed.name, removed.version), id)
                }
            }
            deleted
        }
    }

    suspend fun listDefinitions(limit: Int = 50, offset: Int = 0): List<SagaDefinitionRecord> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.select(
                SAGA_DEFINITION.ID,
                SAGA_DEFINITION.NAME,
                SAGA_DEFINITION.VERSION,
                SAGA_DEFINITION.DEFINITION,
                SAGA_DEFINITION.CREATED_AT,
                SAGA_DEFINITION.UPDATED_AT,
            )
                .from(SAGA_DEFINITION)
                .orderBy(SAGA_DEFINITION.UPDATED_AT.desc())
                .limit(limit)
                .offset(offset)
                .fetch()
                .map { record ->
                    SagaDefinitionRecord(
                        id = record.get(SAGA_DEFINITION.ID) ?: UUID.randomUUID(),
                        name = record.get(SAGA_DEFINITION.NAME) ?: "",
                        version = record.get(SAGA_DEFINITION.VERSION) ?: "",
                        definitionJson = record.get(SAGA_DEFINITION.DEFINITION)?.data() ?: "",
                        createdAt = record.get(SAGA_DEFINITION.CREATED_AT).toInstant(),
                        updatedAt = record.get(SAGA_DEFINITION.UPDATED_AT).toInstant(),
                    )
                }
                .also { list -> list.forEach { putDefinitionInCache(it) } }
        }
    }



    /**
     * One multi-row INSERT for step results buffered in Redis by versions before 2.1. Each row keeps the
     * createdAt recorded when the step actually completed: stamping them all with the flush time
     * made /steps ordering arbitrary and inflated latencyMs (created_at - step_started_at).
     */
    private fun insertStepResultBatch(dsl: org.jooq.DSLContext, sagaId: UUID, steps: List<RedisStepEntry>) {
        if (steps.isEmpty()) return
        var insert = dsl.insertInto(
            SAGA_STEP_RESULT,
            SAGA_STEP_RESULT.SAGA_ID,
            SAGA_STEP_RESULT.STEP_INDEX,
            SAGA_STEP_RESULT.STEP_NAME,
            SAGA_STEP_RESULT.PHASE,
            SAGA_STEP_RESULT.STATUS_CODE,
            SAGA_STEP_RESULT.SUCCESS,
            SAGA_STEP_RESULT.RESPONSE_BODY,
            SAGA_STEP_RESULT.STEP_STARTED_AT,
            SAGA_STEP_RESULT.STARTED_AT,
            SAGA_STEP_RESULT.CREATED_AT,
        )
        // Redis keeps steps newest-first (LPUSH); insert oldest-first so the id tie-breaker agrees.
        for (step in steps.sortedBy { it.createdAt }) {
            insert = insert.values(
                sagaId, step.stepIndex, step.stepName, step.phase,
                step.statusCode, step.success, step.responseBody?.let { toJsonb(it) },
                step.stepStartedAt.takeIf { it != Instant.EPOCH }?.toOffset(),
                step.startedAt.toOffset(), step.createdAt.toOffset(),
            )
        }
        insert.execute()
    }

    /** Hot path (once per execution slice): plain JDBC batch, no query rendering. */
    suspend fun insertStepCalls(calls: List<StepCallEntry>) {
        if (calls.isEmpty()) return
        db.withConnection { connection ->
            val sql = """
                INSERT INTO saga_step_call
                    (saga_id, step_name, phase, attempt, request_url, request_body, status_code,
                     response_body, error, step_started_at, created_at, started_at)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?::jsonb, ?, ?, now(), ?)
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                for (call in calls) {
                    ps.setObject(1, call.sagaId)
                    ps.setString(2, call.stepName)
                    ps.setString(3, call.phase.name)
                    ps.setInt(4, call.attempt)
                    ps.setString(5, call.requestUrl)
                    ps.setString(6, call.requestBody?.let { toJsonb(it).data() })
                    if (call.statusCode != null) ps.setInt(7, call.statusCode) else ps.setNull(7, java.sql.Types.INTEGER)
                    ps.setString(8, call.responseBody?.let { toJsonb(it).data() })
                    ps.setString(9, call.error)
                    ps.setObject(10, call.stepStartedAt.toOffset())
                    ps.setObject(11, call.sagaStartedAt.toOffset())
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    suspend fun getStepCalls(sagaId: UUID): List<SagaStepCallRecord> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.select(
                SAGA_STEP_CALL.ID,
                SAGA_STEP_CALL.STEP_NAME,
                SAGA_STEP_CALL.PHASE,
                SAGA_STEP_CALL.ATTEMPT,
                SAGA_STEP_CALL.REQUEST_URL,
                SAGA_STEP_CALL.REQUEST_BODY,
                SAGA_STEP_CALL.STATUS_CODE,
                SAGA_STEP_CALL.RESPONSE_BODY,
                SAGA_STEP_CALL.ERROR,
                SAGA_STEP_CALL.STEP_STARTED_AT,
                SAGA_STEP_CALL.CREATED_AT,
            )
                .from(SAGA_STEP_CALL)
                .where(SAGA_STEP_CALL.SAGA_ID.eq(sagaId))
                .and(SAGA_STEP_CALL.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_STEP_CALL.STEP_STARTED_AT.asc())
                .fetch()
                .map { record ->
                    SagaStepCallRecord(
                        id = record.get(SAGA_STEP_CALL.ID) ?: 0L,
                        stepName = record.get(SAGA_STEP_CALL.STEP_NAME) ?: "",
                        phase = record.get(SAGA_STEP_CALL.PHASE) ?: ExecutionPhase.UP.name,
                        attempt = record.get(SAGA_STEP_CALL.ATTEMPT) ?: 0,
                        requestUrl = record.get(SAGA_STEP_CALL.REQUEST_URL),
                        requestBody = record.get(SAGA_STEP_CALL.REQUEST_BODY)?.data(),
                        statusCode = record.get(SAGA_STEP_CALL.STATUS_CODE),
                        responseBody = record.get(SAGA_STEP_CALL.RESPONSE_BODY)?.data(),
                        error = record.get(SAGA_STEP_CALL.ERROR),
                        stepStartedAt = record.get(SAGA_STEP_CALL.STEP_STARTED_AT).toInstant(),
                        createdAt = record.get(SAGA_STEP_CALL.CREATED_AT).toInstant(),
                    )
                }
        }
    }

    suspend fun getStepResults(sagaId: UUID): List<SagaStepResultRecord> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.select(
                SAGA_STEP_RESULT.ID,
                SAGA_STEP_RESULT.STEP_INDEX,
                SAGA_STEP_RESULT.STEP_NAME,
                SAGA_STEP_RESULT.PHASE,
                SAGA_STEP_RESULT.STATUS_CODE,
                SAGA_STEP_RESULT.SUCCESS,
                SAGA_STEP_RESULT.RESPONSE_BODY,
                SAGA_STEP_RESULT.STEP_STARTED_AT,
                SAGA_STEP_RESULT.STARTED_AT,
                SAGA_STEP_RESULT.CREATED_AT,
            )
                .from(SAGA_STEP_RESULT)
                .where(SAGA_STEP_RESULT.SAGA_ID.eq(sagaId))
                .and(SAGA_STEP_RESULT.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_STEP_RESULT.CREATED_AT.asc(), SAGA_STEP_RESULT.ID.asc())
                .fetch()
                .map { record ->
                    SagaStepResultRecord(
                        id = record.get(SAGA_STEP_RESULT.ID) ?: 0L,
                        stepIndex = record.get(SAGA_STEP_RESULT.STEP_INDEX) ?: 0,
                        stepName = record.get(SAGA_STEP_RESULT.STEP_NAME) ?: "",
                        phase = record.get(SAGA_STEP_RESULT.PHASE) ?: ExecutionPhase.UP.name,
                        statusCode = record.get(SAGA_STEP_RESULT.STATUS_CODE),
                        success = record.get(SAGA_STEP_RESULT.SUCCESS) ?: false,
                        responseBody = record.get(SAGA_STEP_RESULT.RESPONSE_BODY)?.data(),
                        startedAt = record.get(SAGA_STEP_RESULT.STARTED_AT).toInstant(),
                        stepStartedAt = record.get(SAGA_STEP_RESULT.STEP_STARTED_AT)?.toInstant(),
                        createdAt = record.get(SAGA_STEP_RESULT.CREATED_AT).toInstant(),
                    )
                }
        }
    }

    suspend fun listExecutions(
        status: String? = null,
        name: String? = null,
        limit: Int = 50,
        offset: Int = 0,
    ): List<SagaExecutionSummaryRecord> {
        val listCutoff = Instant.now().minus(30, ChronoUnit.DAYS).toOffset()
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            var query = dsl.select(
                SAGA_EXECUTION.ID,
                SAGA_EXECUTION.NAME,
                SAGA_EXECUTION.VERSION,
                SAGA_EXECUTION.STATUS,
                SAGA_EXECUTION.FAILURE_DESCRIPTION,
                SAGA_EXECUTION.STARTED_AT,
                SAGA_EXECUTION.COMPLETED_AT,
                SAGA_EXECUTION.UPDATED_AT,
            )
                .from(SAGA_EXECUTION)
                .where(SAGA_EXECUTION.STARTED_AT.ge(listCutoff))
            if (status != null) query = query.and(SAGA_EXECUTION.STATUS.eq(status))
            if (name != null) query = query.and(SAGA_EXECUTION.NAME.eq(name))
            query.orderBy(SAGA_EXECUTION.STARTED_AT.desc())
                .limit(limit)
                .offset(offset)
                .fetch()
                .map { record ->
                    SagaExecutionSummaryRecord(
                        id = record.get(SAGA_EXECUTION.ID) ?: UUID.randomUUID(),
                        name = record.get(SAGA_EXECUTION.NAME) ?: "",
                        version = record.get(SAGA_EXECUTION.VERSION) ?: "",
                        status = record.get(SAGA_EXECUTION.STATUS) ?: "UNKNOWN",
                        failureDescription = record.get(SAGA_EXECUTION.FAILURE_DESCRIPTION),
                        startedAt = record.get(SAGA_EXECUTION.STARTED_AT).toInstant(),
                        completedAt = record.get(SAGA_EXECUTION.COMPLETED_AT)?.toInstant(),
                        updatedAt = record.get(SAGA_EXECUTION.UPDATED_AT).toInstant(),
                    )
                }
        }
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    private fun freshCachedDefinition(id: UUID): SagaDefinitionRecord? {
        val cached = definitionCache[id] ?: return null
        if (System.nanoTime() - cached.cachedAtNanos >= definitionCacheTtlNanos) {
            definitionCache.remove(id)
            return null
        }
        return cached.record
    }

    private fun putDefinitionInCache(record: SagaDefinitionRecord) {
        if (definitionCacheTtlNanos <= 0) return
        definitionCache[record.id] = CachedDefinition(record, System.nanoTime())
        definitionNameVersionCache[definitionNameVersionKey(record.name, record.version)] = record.id
    }

    private fun definitionNameVersionKey(name: String, version: String) = "$name::$version"

    // ── Checkpoints ────────────────────────────────────────────────────────────
    // Every execution's resume point lives in its saga_execution row (changeset 009). All writes
    // to it are compare-and-set on checkpoint_seq, in one statement each: a worker acting on an
    // outdated copy of an execution (redelivered after its claim expired, or paused past it)
    // matches zero rows instead of overwriting newer progress.

    /** Creates rows for executions about to be enqueued for the first time; existing rows are kept. */
    suspend fun admitExecutions(executions: List<SagaExecution>) {
        if (executions.isEmpty()) return
        db.withConnection { connection ->
            val sql = """
                INSERT INTO saga_execution
                    (id, name, version, definition, status, started_at, updated_at, payload,
                     checkpoint, checkpoint_seq, checkpoint_carrier, resume_at)
                VALUES (?, ?, ?, ?::jsonb, 'IN_PROGRESS', ?, now(), ?::jsonb, ?::jsonb, ?, ?, now())
                ON CONFLICT (id, started_at) DO NOTHING
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                for (execution in executions) {
                    ps.setObject(1, execution.id)
                    ps.setString(2, execution.definition.name)
                    ps.setString(3, execution.definition.version)
                    ps.setString(4, execution.persistedDefinitionJson())
                    ps.setObject(5, execution.startedAt.toOffset())
                    ps.setString(6, execution.persistedPayloadJson())
                    ps.setString(7, checkpointJson(execution))
                    ps.setLong(8, execution.checkpointSeq)
                    ps.setLong(9, execution.checkpointSeq)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    /**
     * Imports an execution's state that a pre-2.1 version buffered in Redis: makes sure its row
     * exists, then adds the buffered step results and failure details.
     */
    suspend fun importLegacyState(
        execution: SagaExecution,
        steps: List<RedisStepEntry>,
        failureDescription: String?,
        callbackWarning: String?,
    ) {
        admitExecutions(listOf(execution))
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            insertStepResultBatch(dsl, execution.id, steps)
            if (failureDescription != null || callbackWarning != null) {
                dsl.update(SAGA_EXECUTION)
                    .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, DSL.coalesce(DSL.`val`(failureDescription), SAGA_EXECUTION.FAILURE_DESCRIPTION))
                    .set(SAGA_EXECUTION.CALLBACK_WARNING, DSL.coalesce(DSL.`val`(callbackWarning), SAGA_EXECUTION.CALLBACK_WARNING))
                    .where(SAGA_EXECUTION.ID.eq(execution.id))
                    .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                    .execute()
            }
        }
    }

    suspend fun readCheckpoint(executionId: UUID): PersistedCheckpoint? =
        db.withConnection { connection ->
            val sql = """
                SELECT status, checkpoint_seq, checkpoint_carrier, updated_at, checkpoint IS NULL AS legacy
                FROM saga_execution WHERE id = ? AND started_at >= ?
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setObject(1, executionId)
                ps.setObject(2, cutoff())
                ps.executeQuery().use { rs ->
                    if (!rs.next()) return@withConnection null
                    PersistedCheckpoint(
                        status = rs.getString(1),
                        seq = rs.getLong(2),
                        carrier = rs.getLong(3).takeUnless { rs.wasNull() },
                        updatedAt = rs.getObject(4, OffsetDateTime::class.java).toInstant(),
                        legacy = rs.getBoolean(5),
                    )
                }
            }
        }

    suspend fun loadCheckpoint(executionId: UUID): SagaExecution? =
        db.withConnection { connection ->
            val sql = """
                SELECT checkpoint, checkpoint_seq, definition, payload, name, version
                FROM saga_execution WHERE id = ? AND started_at >= ? AND checkpoint IS NOT NULL
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setObject(1, executionId)
                ps.setObject(2, cutoff())
                ps.executeQuery().use { rs -> if (rs.next()) rowExecution(rs, executionId) else null }
            }
        }

    /**
     * Compare-and-set from `next.checkpointSeq - 1` to [next], recording [step] and the parked
     * state in the same statement. Returns false when no row matched: the stored seq moved on,
     * or the execution is already terminal.
     */
    suspend fun checkpoint(
        next: SagaExecution,
        carrier: Long,
        resumeAt: Instant,
        step: StepRecord?,
        parking: Parking?,
    ): Boolean {
        val waitingState = parking?.let { parkedStateJson(next, it) }
        return db.withConnection { connection ->
            val sql = """
                WITH upd AS (
                    UPDATE saga_execution
                    SET checkpoint = ?::jsonb, checkpoint_seq = ?, checkpoint_carrier = ?, resume_at = ?,
                        status = ?, waiting_state = COALESCE(?::jsonb, waiting_state), updated_at = now()
                    WHERE id = ? AND started_at >= ? AND checkpoint_seq = ?
                      AND status NOT IN ('SUCCEEDED', 'FAILED', 'CORRUPTED')
                    RETURNING id, started_at
                ), ins AS (
                    INSERT INTO saga_step_result
                        (saga_id, step_index, step_name, phase, status_code, success, response_body,
                         step_started_at, started_at, created_at)
                    SELECT id, ?, ?, ?, ?, ?, ?::jsonb, ?, started_at, now() FROM upd WHERE ?
                )
                SELECT count(*) FROM upd
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setString(1, checkpointJson(next))
                ps.setLong(2, next.checkpointSeq)
                ps.setLong(3, carrier)
                ps.setObject(4, resumeAt.toOffset())
                ps.setString(5, parking?.status ?: "IN_PROGRESS")
                ps.setString(6, waitingState)
                ps.setObject(7, next.id)
                ps.setObject(8, cutoff())
                ps.setLong(9, next.checkpointSeq - 1)
                ps.setInt(10, step?.stepIdx ?: 0)
                ps.setString(11, step?.stepName)
                ps.setString(12, step?.phase?.name)
                if (step?.statusCode != null) ps.setInt(13, step.statusCode) else ps.setNull(13, java.sql.Types.INTEGER)
                ps.setBoolean(14, step?.success ?: false)
                ps.setString(15, step?.responseBody?.let { toJsonb(it).data() })
                ps.setObject(16, step?.stepStartedAt?.toOffset())
                ps.setBoolean(17, step != null)
                ps.executeQuery().use { rs -> rs.next() && rs.getLong(1) == 1L }
            }
        }
    }

    /**
     * Records a terminal status as a compare-and-set on [expectedSeq]; clears the resume state.
     * A null [failureDescription] keeps the one recorded earlier (e.g. by the failure that led to
     * compensation). Returns false when no row matched.
     */
    suspend fun finalizeCheckpointed(id: UUID, expectedSeq: Long, status: String, failureDescription: String?): Boolean =
        db.withConnection { connection ->
            val sql = """
                UPDATE saga_execution
                SET status = ?, failure_description = COALESCE(?, failure_description),
                    completed_at = now(), updated_at = now(), checkpoint = NULL, checkpoint_seq = ?,
                    checkpoint_carrier = NULL, resume_at = NULL, waiting_state = NULL
                WHERE id = ? AND started_at >= ? AND checkpoint_seq = ?
                  AND status NOT IN ('SUCCEEDED', 'FAILED', 'CORRUPTED')
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setString(1, status)
                ps.setString(2, failureDescription)
                ps.setLong(3, expectedSeq + 1)
                ps.setObject(4, id)
                ps.setObject(5, cutoff())
                ps.setLong(6, expectedSeq)
                ps.executeUpdate() == 1
            }
        }

    /**
     * Turns a FAILED execution back into a running one starting from [execution]'s state, for the
     * retry endpoint. Bumps the seq so any leftover copy of the old run is fenced off, and returns
     * the execution carrying it; null when the row is not (or no longer) FAILED.
     */
    suspend fun prepareRetry(execution: SagaExecution): SagaExecution? =
        db.withConnection { connection ->
            val sql = """
                UPDATE saga_execution
                SET status = 'IN_PROGRESS', failure_description = NULL, callback_warning = NULL,
                    last_failed_step_index = NULL, last_failed_phase = NULL, waiting_state = NULL,
                    checkpoint = ?::jsonb, checkpoint_seq = checkpoint_seq + 1,
                    checkpoint_carrier = checkpoint_seq + 1, resume_at = now(), updated_at = now()
                WHERE id = ? AND started_at >= ? AND status = 'FAILED'
                RETURNING checkpoint_seq
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setString(1, checkpointJson(execution))
                ps.setObject(2, execution.id)
                ps.setObject(3, cutoff())
                ps.executeQuery().use { rs -> if (rs.next()) execution.copy(checkpointSeq = rs.getLong(1)) else null }
            }
        }

    /**
     * Re-sends an execution whose queue item vanished: claims up to [limit] rows matching
     * [condition] (rows locked by another pod are skipped), bumps their seq so any copy still
     * around becomes stale, and returns them rebuilt at that seq.
     */
    private suspend fun claimForRedelivery(condition: String, conditionParam: Any, limit: Int): List<SagaExecution> =
        db.withConnection { connection ->
            val sql = """
                UPDATE saga_execution se
                SET checkpoint_seq = se.checkpoint_seq + 1, checkpoint_carrier = se.checkpoint_seq + 1,
                    resume_at = now(), updated_at = now()
                FROM (
                    SELECT id, started_at FROM saga_execution
                    WHERE started_at >= ? AND ($condition)
                    LIMIT ? FOR UPDATE SKIP LOCKED
                ) c
                WHERE se.id = c.id AND se.started_at = c.started_at
                RETURNING se.id, se.checkpoint, se.checkpoint_seq, se.definition, se.payload, se.name, se.version, se.waiting_state
            """.trimIndent()
            connection.prepareStatement(sql).use { ps ->
                ps.setObject(1, cutoff())
                ps.setObject(2, conditionParam)
                ps.setInt(3, limit)
                ps.executeQuery().use { rs ->
                    val claimed = mutableListOf<SagaExecution>()
                    while (rs.next()) {
                        val id = rs.getObject("id", UUID::class.java)
                        val execution = if (rs.getString("checkpoint") != null) {
                            rowExecution(rs, id)
                        } else {
                            // Parked before checkpoints existed: the parked state holds the execution.
                            legacyWaitingExecution(rs.getString("waiting_state"))?.copy(checkpointSeq = rs.getLong("checkpoint_seq"))
                        }
                        if (execution != null) claimed += execution
                    }
                    claimed
                }
            }
        }

    /**
     * Running or sleeping executions that should have moved more than [staleAfterMillis] ago but
     * did not: their queue item was lost (Redis data loss) or their worker died after a checkpoint
     * without handing the work on.
     */
    override suspend fun claimStalledExecutions(staleAfterMillis: Long, limit: Int): List<SagaExecution> =
        claimForRedelivery(
            "status IN ('IN_PROGRESS', 'SLEEPING') AND checkpoint IS NOT NULL " +
                "AND GREATEST(updated_at, COALESCE(resume_at, updated_at)) < now() - make_interval(secs => ?)",
            staleAfterMillis / 1000.0,
            limit,
        )

    /** Executions still waiting for a callback [bufferSeconds] after its deadline. */
    override suspend fun claimExpiredCallbackWaits(bufferSeconds: Long, limit: Int): List<SagaExecution> =
        claimForRedelivery(
            "status = 'WAITING_CALLBACK' " +
                "AND COALESCE(resume_at, (waiting_state->>'expiresAt')::timestamptz) < now() - make_interval(secs => ?)",
            bufferSeconds.toDouble(),
            limit,
        )

    private fun rowExecution(rs: java.sql.ResultSet, id: UUID): SagaExecution? = runCatching {
        val state = json.decodeFromString(CheckpointJson.serializer(), rs.getString("checkpoint"))
        val definitionJson = rs.getString("definition")
        val name = rs.getString("name")
        val version = rs.getString("version")
        val definitionV2 = if (json.parseToJsonElement(definitionJson).let { it is JsonObject && it.containsKey("nodes") }) {
            json.decodeFromString(SagaDefinitionV2.serializer(), definitionJson)
        } else null
        val definition = if (definitionV2 != null) {
            // Same name/version stub a v2 execution is created with (see Application.kt).
            SagaDefinition(name = name, version = version, failureHandling = definitionV2.failureHandling, steps = emptyList())
        } else {
            json.decodeFromString(SagaDefinition.serializer(), definitionJson)
        }
        val payload = rs.getString("payload")?.let { raw ->
            (json.parseToJsonElement(raw) as? JsonObject)?.mapValues { PayloadValue(it.value) }
        } ?: emptyMap()
        SagaExecution(
            definition = definition,
            definitionV2 = definitionV2,
            id = id,
            startedAt = state.startedAt,
            currentStepIndex = state.currentStepIndex,
            state = state.state,
            payload = payload,
            parentExecutionId = state.parentExecutionId,
            parentStartedAt = state.parentStartedAt,
            parentSplitNodeId = state.parentSplitNodeId,
            parentJoinNodeId = state.parentJoinNodeId,
            branchId = state.branchId,
            checkpointSeq = rs.getLong("checkpoint_seq"),
        )
    }.getOrNull()

    private fun legacyWaitingExecution(raw: String?): SagaExecution? = raw?.let {
        runCatching {
            json.decodeFromString(SagaExecution.serializer(), json.decodeFromString(WaitingStateJson.serializer(), it).executionJson)
        }.getOrNull()
    }

    private fun checkpointJson(execution: SagaExecution): String =
        json.encodeToString(
            CheckpointJson.serializer(),
            CheckpointJson(
                startedAt = execution.startedAt,
                currentStepIndex = execution.currentStepIndex,
                state = execution.state,
                parentExecutionId = execution.parentExecutionId,
                parentStartedAt = execution.parentStartedAt,
                parentSplitNodeId = execution.parentSplitNodeId,
                parentJoinNodeId = execution.parentJoinNodeId,
                branchId = execution.branchId,
            ),
        )

    /** The waiting_state document for a parked checkpoint, in the shape its readers expect. */
    private fun parkedStateJson(execution: SagaExecution, parking: Parking): String {
        val executionJson = json.encodeToString(SagaExecution.serializer(), execution)
        return when (parking) {
            is Parking.Callback -> {
                val state = execution.state as ExecutionState.WaitingCallback
                json.encodeToString(
                    WaitingStateJson.serializer(),
                    WaitingStateJson(state.nodeId, state.attempt, state.nonce, parking.signature, state.deadlineAt, executionJson),
                )
            }
            is Parking.Sleep -> json.encodeToString(SleepStateJson.serializer(), SleepStateJson(parking.wakeAt, executionJson))
            Parking.Join -> {
                val state = execution.state as ExecutionState.WaitingJoin
                json.encodeToString(WaitingJoinStateJson.serializer(), WaitingJoinStateJson(state.splitNodeId, state.joinNodeId, executionJson))
            }
        }
    }

    /**
     * The checkpoint column: everything needed to resume an execution except what has its own
     * column (definition, payload), so per-node checkpoints stay small.
     */
    @Serializable
    private data class CheckpointJson(
        @Serializable(with = InstantAsStringSerializer::class)
        val startedAt: Instant,
        val currentStepIndex: Int,
        val state: ExecutionState,
        val parentExecutionId: @Serializable(with = UuidAsStringSerializer::class) UUID? = null,
        val parentStartedAt: @Serializable(with = InstantAsStringSerializer::class) Instant? = null,
        val parentSplitNodeId: String? = null,
        val parentJoinNodeId: String? = null,
        val branchId: String? = null,
    )

    private fun parseJson(raw: String): JsonElement? = runCatching { json.parseToJsonElement(raw) }.getOrNull()

    private fun toJsonb(raw: String): JSONB = runCatching {
        json.parseToJsonElement(raw)
        JSONB.valueOf(raw)
    }.getOrElse { JSONB.valueOf(json.encodeToString(String.serializer(), raw)) }

    private fun cutoff(): OffsetDateTime = Instant.now().minus(15, ChronoUnit.DAYS).toOffset()

    // ── Data classes ───────────────────────────────────────────────────────────

    data class SagaExecutionStatus(
        val id: UUID,
        val name: String,
        val version: String,
        val definition: String?,
        val status: String,
        val failureDescription: String?,
        val callbackWarning: String?,
        val lastFailedStepIndex: Int?,
        val lastFailedPhase: String?,
        val startedAt: Instant,
        val completedAt: Instant?,
        val updatedAt: Instant,
    )

    data class SagaExecutionRetryData(
        val id: UUID,
        val definitionJson: String?,
        val failedStepIndex: Int?,
        val failedPhase: String?,
        val startedAt: Instant,
        val status: String,
        val payloadJson: String?,
    )

    data class SagaDefinitionRecord(
        val id: UUID,
        val name: String,
        val version: String,
        val definitionJson: String,
        val createdAt: Instant,
        val updatedAt: Instant,
    )

    data class SagaStepCallRecord(
        val id: Long,
        val stepName: String,
        val phase: String,
        val attempt: Int,
        val requestUrl: String?,
        val requestBody: String?,
        val statusCode: Int?,
        val responseBody: String?,
        val error: String?,
        val stepStartedAt: Instant,
        val createdAt: Instant,
    )

    data class SagaExecutionSummaryRecord(
        val id: UUID,
        val name: String,
        val version: String,
        val status: String,
        val failureDescription: String?,
        val startedAt: Instant,
        val completedAt: Instant?,
        val updatedAt: Instant,
    )

    data class SagaStepResultRecord(
        val id: Long,
        val stepIndex: Int,
        val stepName: String,
        val phase: String,
        val statusCode: Int?,
        val success: Boolean,
        val responseBody: String?,
        val startedAt: Instant,
        val stepStartedAt: Instant?,
        val createdAt: Instant,
    )

    @Serializable
    private data class WaitingStateJson(
        val nodeId: String,
        val attempt: Int,
        val nonce: String,
        val signature: String,
        @Serializable(with = InstantAsStringSerializer::class)
        val expiresAt: Instant,
        val executionJson: String,
    )

    @Serializable
    private data class SleepStateJson(
        @Serializable(with = InstantAsStringSerializer::class)
        val wakeAt: Instant,
        val executionJson: String,
    )

    @Serializable
    private data class WaitingJoinStateJson(
        val splitNodeId: String,
        val joinNodeId: String,
        val executionJson: String,
    )
}
