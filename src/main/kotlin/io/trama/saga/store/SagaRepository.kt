package io.trama.saga.store

import io.trama.saga.ExecutionPhase
import io.trama.saga.SagaDefinition
import io.trama.saga.SagaExecution
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import org.jooq.JSONB
import org.jooq.impl.DSL

class SagaRepository(
    private val db: DatabaseClient,
) {
    private val definitionCache = java.util.concurrent.ConcurrentHashMap<UUID, SagaDefinitionRecord>()
    private val definitionNameVersionCache = java.util.concurrent.ConcurrentHashMap<String, UUID>()
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

    private val defTable = DSL.table("saga_definition")
    private val defId = DSL.field("id", UUID::class.java)
    private val defName = DSL.field("name", String::class.java)
    private val defVersion = DSL.field("version", String::class.java)
    private val defBody = DSL.field("definition", JSONB::class.java)
    private val defCreatedAt = DSL.field("created_at", Instant::class.java)
    private val defUpdatedAt = DSL.field("updated_at", Instant::class.java)

    private val stepSagaId = DSL.field("saga_id", UUID::class.java)
    private val stepIndex = DSL.field("step_index", Int::class.java)
    private val stepName = DSL.field("step_name", String::class.java)
    private val stepPhase = DSL.field("phase", String::class.java)
    private val stepStatusCode = DSL.field("status_code", Int::class.java)
    private val stepSuccess = DSL.field("success", Boolean::class.java)
    private val stepBody = DSL.field("response_body", JSONB::class.java)
    private val stepStartedAt = DSL.field("started_at", Instant::class.java)
    private val stepCreatedAt = DSL.field("created_at", Instant::class.java)

    fun upsertExecutionStart(execution: SagaExecution) {
        db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val now = Instant.now()
            val definitionJson = JSONB.valueOf(json.encodeToString(SagaDefinition.serializer(), execution.definition))
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
                    execution.id,
                    execution.definition.name,
                    execution.definition.version,
                    definitionJson,
                    "IN_PROGRESS",
                    execution.startedAt,
                    now,
                )
                .onConflict(execId, execStartedAt)
                .doUpdate()
                .set(execStatus, "IN_PROGRESS")
                .set(execDefinition, DSL.coalesce(execTable.field(execDefinition), definitionJson))
                .set(execUpdatedAt, now)
                .execute()
        }
    }

    fun updateExecutionFinal(
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

    fun updateFailureDescription(
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

    fun updateCallbackWarning(executionId: UUID, warning: String) {
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

    fun insertStepResult(
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
                    stepName,
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

    fun loadStepResultsForTemplate(sagaId: UUID): List<StepResultForTemplate> {
        return db.withConnection { connection ->
            val dsl = DSL.using(connection)
            val records = dsl.select(
                stepIndex,
                stepName,
                stepPhase,
                stepBody,
                stepCreatedAt,
            )
                .from(stepTable)
                .where(stepSagaId.eq(sagaId))
                .and(stepStartedAt.ge(cutoff()))
                .orderBy(stepIndex.asc(), stepCreatedAt.desc())
                .fetch()

            val latestByIndex = linkedMapOf<Int, StepResultForTemplate>()
            for (record in records) {
                val index = record.get(stepIndex) ?: continue
                val name = record.get(stepName) ?: ""
                val phase = record.get(stepPhase) ?: ExecutionPhase.UP.name
                val body = record.get(stepBody)?.data()?.let { parseJson(it) }
                val current = latestByIndex[index]
                if (current == null) {
                    latestByIndex[index] = StepResultForTemplate(
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

    fun getExecutionStatus(sagaId: UUID): SagaExecutionStatus? {
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

    fun markRetrying(executionId: UUID) {
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

    data class StepResultForTemplate(
        val index: Int,
        val name: String,
        val upBody: JsonElement?,
        val downBody: JsonElement?,
    )

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

    fun getExecutionForRetry(sagaId: UUID): SagaExecutionRetryData? {
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

    fun insertDefinition(id: UUID, name: String, version: String, definitionJson: String): Boolean {
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
                .onConflict(defId)
                .doNothing()
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

    fun getDefinition(id: UUID): SagaDefinitionRecord? {
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

    fun getDefinitionByNameVersion(name: String, version: String): SagaDefinitionRecord? {
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

    fun deleteDefinition(id: UUID): Boolean {
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

    fun listDefinitions(): List<SagaDefinitionRecord> {
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
}
