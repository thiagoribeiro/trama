package run.trama.saga.store

import run.trama.saga.ExecutionPhase
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.StepCallEntry
import run.trama.saga.WaitingInfo
import run.trama.runtime.CallbackTimeoutRepository
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
import org.jooq.JSONB
import org.jooq.impl.DSL
import run.trama.saga.InstantAsStringSerializer
import run.trama.saga.StepResult
import run.trama.saga.redis.RedisStepEntry

private fun Instant.toOffset(): OffsetDateTime = atOffset(ZoneOffset.UTC)
private fun OffsetDateTime?.toInstant(): Instant = this?.toInstant() ?: Instant.EPOCH

class SagaRepository(
    private val db: DatabaseClient,
    definitionCacheMaxSize: Int = 1000,
) : CallbackTimeoutRepository {
    private val definitionCache: MutableMap<UUID, SagaDefinitionRecord> =
        java.util.Collections.synchronizedMap(
            object : java.util.LinkedHashMap<UUID, SagaDefinitionRecord>(
                minOf(definitionCacheMaxSize, 16), 0.75f, true
            ) {
                override fun removeEldestEntry(eldest: Map.Entry<UUID, SagaDefinitionRecord>) =
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

    suspend fun upsertExecutionStart(execution: SagaExecution) {
        upsertExecutionRecord(
            id = execution.id,
            name = execution.definition.name,
            version = execution.definition.version,
            definitionJson = json.encodeToString(SagaDefinition.serializer(), execution.definition),
            startedAt = execution.startedAt,
        )
    }

    /**
     * Ensures a row exists in `saga_execution` for the given [id]/[startedAt] combination.
     * On conflict, preserves any existing definition and resets status to `IN_PROGRESS`.
     */
    suspend fun upsertExecutionRecord(
        id: UUID,
        name: String,
        version: String,
        definitionJson: String,
        startedAt: Instant,
    ) {
        val definitionJsonb = JSONB.valueOf(definitionJson)
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
                )
                .values(
                    id,
                    name,
                    version,
                    definitionJsonb,
                    "IN_PROGRESS",
                    startedAt.toOffset(),
                    now,
                )
                .onConflict(SAGA_EXECUTION.ID, SAGA_EXECUTION.STARTED_AT)
                .doUpdate()
                .set(SAGA_EXECUTION.STATUS, "IN_PROGRESS")
                .set(SAGA_EXECUTION.DEFINITION,
                    DSL.coalesce(SAGA_EXECUTION.DEFINITION, definitionJsonb))
                .set(SAGA_EXECUTION.UPDATED_AT, now)
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

    suspend fun loadStepResultsForTemplate(sagaId: UUID): List<StepResult> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val records = dsl.select(
                SAGA_STEP_RESULT.STEP_INDEX,
                SAGA_STEP_RESULT.STEP_NAME,
                SAGA_STEP_RESULT.PHASE,
                SAGA_STEP_RESULT.RESPONSE_BODY,
                SAGA_STEP_RESULT.CREATED_AT,
            )
                .from(SAGA_STEP_RESULT)
                .where(SAGA_STEP_RESULT.SAGA_ID.eq(sagaId))
                .and(SAGA_STEP_RESULT.STARTED_AT.ge(cutoff()))
                .orderBy(SAGA_STEP_RESULT.STEP_INDEX.asc(), SAGA_STEP_RESULT.CREATED_AT.desc())
                .fetch()

            val latestByIndex = linkedMapOf<Int, StepResult>()
            for (record in records) {
                val index = record.get(SAGA_STEP_RESULT.STEP_INDEX) ?: continue
                val name = record.get(SAGA_STEP_RESULT.STEP_NAME) ?: ""
                val phase = record.get(SAGA_STEP_RESULT.PHASE) ?: ExecutionPhase.UP.name
                val body = record.get(SAGA_STEP_RESULT.RESPONSE_BODY)?.data()?.let { parseJson(it) }
                val current = latestByIndex[index]
                if (current == null) {
                    latestByIndex[index] = StepResult(
                        index = index,
                        name = name,
                        upBody = if (phase == ExecutionPhase.UP.name) body else null,
                        downBody = if (phase == ExecutionPhase.DOWN.name) body else null,
                    )
                    continue
                }
                if (phase == ExecutionPhase.UP.name && current.upBody == null) {
                    latestByIndex[index] = current.copy(upBody = body)
                } else if (phase == ExecutionPhase.DOWN.name && current.downBody == null) {
                    latestByIndex[index] = current.copy(downBody = body)
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

    suspend fun markRetrying(executionId: UUID) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.update(SAGA_EXECUTION)
                .set(SAGA_EXECUTION.STATUS, "IN_PROGRESS")
                .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, null as String?)
                .set(SAGA_EXECUTION.CALLBACK_WARNING, null as String?)
                .set(SAGA_EXECUTION.LAST_FAILED_STEP_INDEX, null as Int?)
                .set(SAGA_EXECUTION.LAST_FAILED_PHASE, null as String?)
                .set(SAGA_EXECUTION.UPDATED_AT, Instant.now().toOffset())
                .where(SAGA_EXECUTION.ID.eq(executionId))
                .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                .execute()
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

    override suspend fun consumeWaitingState(executionId: UUID): WaitingInfo? {
        return db.withConnection { connection ->
            // Atomically clear and return waiting_state in one round-trip.
            val sql = """
                UPDATE saga_execution
                SET waiting_state = NULL, updated_at = now()
                WHERE id = ?
                  AND started_at >= ?
                  AND waiting_state IS NOT NULL
                RETURNING waiting_state
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

    override suspend fun findExpiredWaitingExecutions(bufferSeconds: Long, limit: Int): List<UUID> {
        return db.withConnection { connection ->
            val sql = """
                SELECT id FROM saga_execution
                WHERE status = 'WAITING_CALLBACK'
                AND waiting_state IS NOT NULL
                AND (waiting_state->>'expiresAt')::timestamptz < now() - make_interval(secs => ?)
                AND started_at >= ?
                ORDER BY started_at DESC
                LIMIT ?
            """.trimIndent()
            val rs = connection.prepareStatement(sql).also { ps ->
                ps.setLong(1, bufferSeconds)
                ps.setObject(2, java.sql.Timestamp.from(cutoff().toInstant()))
                ps.setInt(3, limit)
            }.executeQuery()
            val ids = mutableListOf<UUID>()
            while (rs.next()) {
                ids += UUID.fromString(rs.getString("id"))
            }
            ids
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
        definitionCache[id]?.let { return it }
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
            definitionCache[id]?.let { return it }
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
                val removed = definitionCache.remove(id)
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
     * Batch-inserts all step results in a single multi-row INSERT, replacing the per-step loop
     * that was used during finalization.
     */
    suspend fun insertStepResults(sagaId: UUID, steps: List<RedisStepEntry>) {
        if (steps.isEmpty()) return
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
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
            for (step in steps) {
                val body = step.responseBody?.let { toJsonb(it) }
                val stepStartedAt = step.stepStartedAt.takeIf { it != Instant.EPOCH }
                insert = insert.values(
                    sagaId,
                    step.stepIndex,
                    step.stepName,
                    step.phase,
                    step.statusCode,
                    step.success,
                    body,
                    stepStartedAt?.toOffset(),
                    step.startedAt.toOffset(),
                    now,
                )
            }
            insert.execute()
        }
    }

    /**
     * Single UPSERT that inserts a new execution row OR updates an existing one with the final
     * status, replacing the `upsertExecutionRecord` + `updateExecutionFinal` pair used during
     * finalization. Saves one DB round-trip per saga completion.
     */
    suspend fun upsertExecutionFinal(
        id: UUID,
        name: String,
        version: String,
        definitionJson: String,
        startedAt: Instant,
        status: String,
        failureDescription: String?,
    ) {
        val definitionJsonb = JSONB.valueOf(definitionJson)
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
            val completedAt = if (status == "IN_PROGRESS") null else now
            dsl.insertInto(SAGA_EXECUTION)
                .columns(
                    SAGA_EXECUTION.ID,
                    SAGA_EXECUTION.NAME,
                    SAGA_EXECUTION.VERSION,
                    SAGA_EXECUTION.DEFINITION,
                    SAGA_EXECUTION.STATUS,
                    SAGA_EXECUTION.FAILURE_DESCRIPTION,
                    SAGA_EXECUTION.STARTED_AT,
                    SAGA_EXECUTION.COMPLETED_AT,
                    SAGA_EXECUTION.UPDATED_AT,
                )
                .values(
                    id,
                    name,
                    version,
                    definitionJsonb,
                    status,
                    failureDescription,
                    startedAt.toOffset(),
                    completedAt,
                    now,
                )
                .onConflict(SAGA_EXECUTION.ID, SAGA_EXECUTION.STARTED_AT)
                .doUpdate()
                .set(SAGA_EXECUTION.STATUS, status)
                .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, failureDescription)
                .set(SAGA_EXECUTION.DEFINITION,
                    DSL.coalesce(SAGA_EXECUTION.DEFINITION, definitionJsonb))
                .set(SAGA_EXECUTION.COMPLETED_AT, completedAt)
                .set(SAGA_EXECUTION.UPDATED_AT, now)
                .execute()
        }
    }

    /**
     * Performs all finalization DB writes in a single connection (one HikariCP checkout).
     * Replaces the sequence: upsertExecutionRecord → updateFailureDescription →
     * updateCallbackWarning → updateExecutionFinal → N×insertStepResult.
     */
    suspend fun finalizeExecution(
        id: UUID,
        name: String,
        version: String,
        definitionJson: String,
        startedAt: Instant,
        status: String,
        failureDescription: String?,
        lastFailedStepIndex: Int?,
        lastFailedPhase: ExecutionPhase?,
        callbackWarning: String?,
        steps: List<RedisStepEntry>,
    ) {
        val definitionJsonb = JSONB.valueOf(definitionJson)
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
            val completedAt = if (status == "IN_PROGRESS") null else now

            // 1. Single UPSERT with final status (replaces upsertExecutionRecord + updateExecutionFinal)
            dsl.insertInto(SAGA_EXECUTION)
                .columns(
                    SAGA_EXECUTION.ID,
                    SAGA_EXECUTION.NAME,
                    SAGA_EXECUTION.VERSION,
                    SAGA_EXECUTION.DEFINITION,
                    SAGA_EXECUTION.STATUS,
                    SAGA_EXECUTION.FAILURE_DESCRIPTION,
                    SAGA_EXECUTION.STARTED_AT,
                    SAGA_EXECUTION.COMPLETED_AT,
                    SAGA_EXECUTION.UPDATED_AT,
                )
                .values(
                    id, name, version, definitionJsonb, status,
                    failureDescription, startedAt.toOffset(), completedAt, now,
                )
                .onConflict(SAGA_EXECUTION.ID, SAGA_EXECUTION.STARTED_AT)
                .doUpdate()
                .set(SAGA_EXECUTION.STATUS, status)
                .set(SAGA_EXECUTION.FAILURE_DESCRIPTION, failureDescription)
                .set(SAGA_EXECUTION.DEFINITION, DSL.coalesce(SAGA_EXECUTION.DEFINITION, definitionJsonb))
                .set(SAGA_EXECUTION.COMPLETED_AT, completedAt)
                .set(SAGA_EXECUTION.UPDATED_AT, now)
                .execute()

            // 2. Failure metadata (conditional)
            if (failureDescription != null) {
                dsl.update(SAGA_EXECUTION)
                    .set(SAGA_EXECUTION.LAST_FAILED_STEP_INDEX, lastFailedStepIndex)
                    .set(SAGA_EXECUTION.LAST_FAILED_PHASE, lastFailedPhase?.name)
                    .set(SAGA_EXECUTION.UPDATED_AT, now)
                    .where(SAGA_EXECUTION.ID.eq(id))
                    .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                    .execute()
            }

            // 3. Callback warning (conditional)
            if (callbackWarning != null) {
                dsl.update(SAGA_EXECUTION)
                    .set(SAGA_EXECUTION.CALLBACK_WARNING, callbackWarning)
                    .set(SAGA_EXECUTION.UPDATED_AT, now)
                    .where(SAGA_EXECUTION.ID.eq(id))
                    .and(SAGA_EXECUTION.STARTED_AT.ge(cutoff()))
                    .execute()
            }

            // 4. Batch insert all step results (replaces N×insertStepResult)
            if (steps.isNotEmpty()) {
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
                for (step in steps) {
                    val body = step.responseBody?.let { toJsonb(it) }
                    val stepStartedAt = step.stepStartedAt.takeIf { it != Instant.EPOCH }
                    insert = insert.values(
                        id, step.stepIndex, step.stepName, step.phase,
                        step.statusCode, step.success, body,
                        stepStartedAt?.toOffset(), step.startedAt.toOffset(), now,
                    )
                }
                insert.execute()
            }
        }
    }

    suspend fun insertStepCalls(calls: List<StepCallEntry>) {
        if (calls.isEmpty()) return
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now().toOffset()
            var insert = dsl.insertInto(
                SAGA_STEP_CALL,
                SAGA_STEP_CALL.SAGA_ID,
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
                SAGA_STEP_CALL.STARTED_AT,
            )
            for (call in calls) {
                insert = insert.values(
                    call.sagaId,
                    call.stepName,
                    call.phase.name,
                    call.attempt,
                    call.requestUrl,
                    call.requestBody?.let { toJsonb(it) },
                    call.statusCode,
                    call.responseBody?.let { toJsonb(it) },
                    call.error,
                    call.stepStartedAt.toOffset(),
                    now,
                    call.sagaStartedAt.toOffset(),
                )
            }
            insert.execute()
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
                .orderBy(SAGA_STEP_RESULT.CREATED_AT.asc())
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

    private fun putDefinitionInCache(record: SagaDefinitionRecord) {
        definitionCache[record.id] = record
        definitionNameVersionCache[definitionNameVersionKey(record.name, record.version)] = record.id
    }

    private fun definitionNameVersionKey(name: String, version: String) = "$name::$version"

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
}
