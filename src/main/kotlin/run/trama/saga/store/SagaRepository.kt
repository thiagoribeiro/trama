package run.trama.saga.store

import run.trama.saga.ExecutionPhase
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaExecution
import run.trama.saga.WaitingInfo
import run.trama.runtime.CallbackTimeoutRepository
import java.time.Instant
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

    private val execTable = DSL.table("saga_execution")
    private val stepTable = DSL.table("saga_step_result")

    private val execId = DSL.field("id", UUID::class.java)
    private val execName = DSL.field("name", String::class.java)
    private val execVersion = DSL.field("version", String::class.java)
    private val execDefinition = DSL.field("definition", JSONB::class.java)
    private val execStatus = DSL.field("status", String::class.java)
    private val execFailure = DSL.field("failure_description", String::class.java)
    private val execCallbackWarning = DSL.field("callback_warning", String::class.java)
    private val execLastFailedStepIndex = DSL.field("last_failed_step_index", Int::class.java)
    private val execLastFailedPhase = DSL.field("last_failed_phase", String::class.java)
    private val execStartedAt = DSL.field("started_at", Instant::class.java)
    private val execCompletedAt = DSL.field("completed_at", Instant::class.java)
    private val execUpdatedAt = DSL.field("updated_at", Instant::class.java)
    private val execWaitingState = DSL.field("waiting_state", JSONB::class.java)

    private val defTable = DSL.table("saga_definition")
    private val defId = DSL.field("id", UUID::class.java)
    private val defName = DSL.field("name", String::class.java)
    private val defVersion = DSL.field("version", String::class.java)
    private val defBody = DSL.field("definition", JSONB::class.java)
    private val defCreatedAt = DSL.field("created_at", Instant::class.java)
    private val defUpdatedAt = DSL.field("updated_at", Instant::class.java)

    private val stepSagaId = DSL.field("saga_id", UUID::class.java)
    private val stepIndex = DSL.field("step_index", Int::class.java)
    private val stepNameField = DSL.field("step_name", String::class.java)
    private val stepPhase = DSL.field("phase", String::class.java)
    private val stepStatusCode = DSL.field("status_code", Int::class.java)
    private val stepSuccess = DSL.field("success", Boolean::class.java)
    private val stepBody = DSL.field("response_body", JSONB::class.java)
    private val stepStartedAt = DSL.field("started_at", Instant::class.java)
    private val stepCreatedAt = DSL.field("created_at", Instant::class.java)

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
     * Prefer this over constructing a dummy [SagaExecution] when only raw metadata is available.
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
            val now = Instant.now()
            dsl.insertInto(execTable)
                .columns(
                    execId,
                    execName,
                    execVersion,
                    execDefinition,
                    execStatus,
                    execStartedAt,
                    execUpdatedAt,
                )
                .values(
                    id,
                    name,
                    version,
                    definitionJsonb,
                    "IN_PROGRESS",
                    startedAt,
                    now,
                )
                .onConflict(execId, execStartedAt)
                .doUpdate()
                .set(execStatus, "IN_PROGRESS")
                .set(execDefinition, DSL.coalesce(execTable.field(execDefinition), definitionJsonb))
                .set(execUpdatedAt, now)
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
            val now = Instant.now()
            dsl.update(execTable)
                .set(execStatus, status)
                .set(execFailure, failureDescription)
                .set(execCompletedAt, now)
                .set(execUpdatedAt, now)
                .where(execId.eq(executionId))
                .and(execStartedAt.ge(cutoff()))
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
            val now = Instant.now()
            dsl.update(execTable)
                .set(execFailure, failureDescription)
                .set(execLastFailedStepIndex, failedStepIndex)
                .set(execLastFailedPhase, failedPhase?.name)
                .set(execUpdatedAt, now)
                .where(execId.eq(executionId))
                .and(execStartedAt.ge(cutoff()))
                .execute()
        }
    }

    suspend fun updateCallbackWarning(executionId: UUID, warning: String) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now()
            dsl.update(execTable)
                .set(execCallbackWarning, warning)
                .set(execUpdatedAt, now)
                .where(execId.eq(executionId))
                .and(execStartedAt.ge(cutoff()))
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
    ) {
        val bodyJson = responseBody?.let { toJsonb(it) }
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.insertInto(stepTable)
                .columns(
                    stepSagaId,
                    stepIndex,
                    stepNameField,
                    stepPhase,
                    stepStatusCode,
                    stepSuccess,
                    stepBody,
                    stepStartedAt,
                    stepCreatedAt,
                )
                .values(
                    sagaId,
                    stepIdx,
                    stepNameValue,
                    phase.name,
                    statusCode,
                    success,
                    bodyJson,
                    startedAt,
                    Instant.now(),
                )
                .execute()
        }
    }

    suspend fun loadStepResultsForTemplate(sagaId: UUID): List<StepResult> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val records = dsl.select(
                stepIndex,
                stepNameField,
                stepPhase,
                stepBody,
                stepCreatedAt,
            )
                .from(stepTable)
                .where(stepSagaId.eq(sagaId))
                .and(stepStartedAt.ge(cutoff()))
                .orderBy(stepIndex.asc(), stepCreatedAt.desc())
                .fetch()

            val latestByIndex = linkedMapOf<Int, StepResult>()
            for (record in records) {
                val index = record.get(stepIndex) ?: continue
                val name = record.get(stepNameField) ?: ""
                val phase = record.get(stepPhase) ?: ExecutionPhase.UP.name
                val body = record.get(stepBody)?.data()?.let { parseJson(it) }
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
                execId,
                execName,
                execVersion,
                execDefinition,
                execStatus,
                execFailure,
                execCallbackWarning,
                execLastFailedStepIndex,
                execLastFailedPhase,
                execStartedAt,
                execCompletedAt,
                execUpdatedAt,
            )
                .from(execTable)
                .where(execId.eq(sagaId))
                .and(execStartedAt.ge(cutoff()))
                .orderBy(execStartedAt.desc())
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaExecutionStatus(
                id = record.get(execId) ?: sagaId,
                name = record.get(execName) ?: "",
                version = record.get(execVersion) ?: "",
                definition = record.get(execDefinition)?.data(),
                status = record.get(execStatus) ?: "UNKNOWN",
                failureDescription = record.get(execFailure),
                callbackWarning = record.get(execCallbackWarning),
                lastFailedStepIndex = record.get(execLastFailedStepIndex),
                lastFailedPhase = record.get(execLastFailedPhase),
                startedAt = record.get(execStartedAt) ?: Instant.EPOCH,
                completedAt = record.get(execCompletedAt),
                updatedAt = record.get(execUpdatedAt) ?: Instant.EPOCH,
            )
        }
    }

    suspend fun markRetrying(executionId: UUID) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now()
            dsl.update(execTable)
                .set(execStatus, "IN_PROGRESS")
                .set(execFailure, null as String?)
                .set(execCallbackWarning, null as String?)
                .set(execLastFailedStepIndex, null as Int?)
                .set(execLastFailedPhase, null as String?)
                .set(execUpdatedAt, now)
                .where(execId.eq(executionId))
                .and(execStartedAt.ge(cutoff()))
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
            dsl.update(execTable)
                .set(execWaitingState, waitingJson)
                .set(execStatus, "WAITING_CALLBACK")
                .set(execUpdatedAt, Instant.now())
                .where(execId.eq(executionId))
                .and(execStartedAt.ge(cutoff()))
                .execute()
        }
    }

    override suspend fun consumeWaitingState(executionId: UUID): WaitingInfo? {
        return db.withConnection { connection ->
            // Atomically clear and return waiting_state in one round-trip (CR-2).
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
                ps.setObject(2, java.sql.Timestamp.from(cutoff()))
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

    /**
     * Returns execution IDs whose `waiting_state` has a `deadlineAt` older than [bufferSeconds]
     * seconds ago. These are executions that should have been processed via the Redis sentinel
     * but were missed (e.g., Redis data loss).
     *
     * @param bufferSeconds grace period — execution whose deadline passed less than this many
     *   seconds ago are skipped to avoid double-processing items still in the Redis ZSET.
     * @param limit maximum rows to return per scan.
     */
    override suspend fun findExpiredWaitingExecutions(bufferSeconds: Long, limit: Int): List<UUID> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
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
                ps.setObject(2, java.sql.Timestamp.from(cutoff()))
                ps.setInt(3, limit)
            }.executeQuery()
            val ids = mutableListOf<UUID>()
            while (rs.next()) {
                ids += UUID.fromString(rs.getString("id"))
            }
            ids
        }
    }

    private fun parseJson(raw: String): JsonElement? {
        return try {
            json.parseToJsonElement(raw)
        } catch (_: Exception) {
            null
        }
    }

    private fun toJsonb(raw: String): JSONB {
        return try {
            json.parseToJsonElement(raw)
            JSONB.valueOf(raw)
        } catch (_: Exception) {
            JSONB.valueOf(json.encodeToString(String.serializer(), raw))
        }
    }

    private fun cutoff(): Instant = Instant.now().minus(15, ChronoUnit.DAYS)

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

    suspend fun getExecutionForRetry(sagaId: UUID): SagaExecutionRetryData? {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                execId,
                execDefinition,
                execLastFailedStepIndex,
                execLastFailedPhase,
                execStartedAt,
            )
                .from(execTable)
                .where(execId.eq(sagaId))
                .and(execStartedAt.ge(cutoff()))
                .orderBy(execStartedAt.desc())
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaExecutionRetryData(
                id = record.get(execId) ?: sagaId,
                definitionJson = record.get(execDefinition)?.data(),
                failedStepIndex = record.get(execLastFailedStepIndex),
                failedPhase = record.get(execLastFailedPhase),
                startedAt = record.get(execStartedAt) ?: Instant.EPOCH,
            )
        }
    }

    data class SagaExecutionRetryData(
        val id: UUID,
        val definitionJson: String?,
        val failedStepIndex: Int?,
        val failedPhase: String?,
        val startedAt: Instant,
    )

    suspend fun insertDefinition(id: UUID, name: String, version: String, definitionJson: String): Boolean {
        val inserted = db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now()
            dsl.insertInto(defTable)
                .columns(
                    defId,
                    defName,
                    defVersion,
                    defBody,
                    defCreatedAt,
                    defUpdatedAt,
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
        if (inserted == 0) {
            return false
        }
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
                defId,
                defName,
                defVersion,
                defBody,
                defCreatedAt,
                defUpdatedAt,
            )
                .from(defTable)
                .where(defId.eq(id))
                .fetchOne()
                ?: return@withConnection null

            SagaDefinitionRecord(
                id = record.get(defId) ?: id,
                name = record.get(defName) ?: "",
                version = record.get(defVersion) ?: "",
                definitionJson = record.get(defBody)?.data() ?: "",
                createdAt = record.get(defCreatedAt) ?: Instant.EPOCH,
                updatedAt = record.get(defUpdatedAt) ?: Instant.EPOCH,
            ).also { putDefinitionInCache(it) }
        }
    }

    suspend fun getDefinitionByNameVersion(name: String, version: String): SagaDefinitionRecord? {
        val key = definitionNameVersionKey(name, version)
        definitionNameVersionCache[key]?.let { id ->
            definitionCache[id]?.let { return it }
            // Defensive cleanup for stale id pointers.
            definitionNameVersionCache.remove(key, id)
        }

        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val record = dsl.select(
                defId,
                defName,
                defVersion,
                defBody,
                defCreatedAt,
                defUpdatedAt,
            )
                .from(defTable)
                .where(defName.eq(name))
                .and(defVersion.eq(version))
                .limit(1)
                .fetchOne()
                ?: return@withConnection null

            SagaDefinitionRecord(
                id = record.get(defId) ?: UUID.randomUUID(),
                name = record.get(defName) ?: "",
                version = record.get(defVersion) ?: "",
                definitionJson = record.get(defBody)?.data() ?: "",
                createdAt = record.get(defCreatedAt) ?: Instant.EPOCH,
                updatedAt = record.get(defUpdatedAt) ?: Instant.EPOCH,
            ).also { putDefinitionInCache(it) }
        }
    }

    suspend fun deleteDefinition(id: UUID): Boolean {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val deleted = dsl.deleteFrom(defTable)
                .where(defId.eq(id))
                .execute() > 0
            if (deleted) {
                val removed = definitionCache.remove(id)
                if (removed != null) {
                    definitionNameVersionCache.remove(definitionNameVersionKey(removed.name, removed.version), id)
                }
            }
            deleted
        }
    }

    suspend fun listDefinitions(limit: Int = 50, offset: Int = 0): List<SagaDefinitionRecord> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            dsl.select(
                defId,
                defName,
                defVersion,
                defBody,
                defCreatedAt,
                defUpdatedAt,
            )
                .from(defTable)
                .orderBy(defUpdatedAt.desc())
                .limit(limit)
                .offset(offset)
                .fetch()
                .map { record ->
                    SagaDefinitionRecord(
                        id = record.get(defId) ?: UUID.randomUUID(),
                        name = record.get(defName) ?: "",
                        version = record.get(defVersion) ?: "",
                        definitionJson = record.get(defBody)?.data() ?: "",
                        createdAt = record.get(defCreatedAt) ?: Instant.EPOCH,
                        updatedAt = record.get(defUpdatedAt) ?: Instant.EPOCH,
                    )
                }
                .also { list ->
                    list.forEach { putDefinitionInCache(it) }
                }
        }
    }

    private fun putDefinitionInCache(record: SagaDefinitionRecord) {
        definitionCache[record.id] = record
        definitionNameVersionCache[definitionNameVersionKey(record.name, record.version)] = record.id
    }

    private fun definitionNameVersionKey(name: String, version: String): String {
        return "$name::$version"
    }

    data class SagaDefinitionRecord(
        val id: UUID,
        val name: String,
        val version: String,
        val definitionJson: String,
        val createdAt: Instant,
        val updatedAt: Instant,
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
