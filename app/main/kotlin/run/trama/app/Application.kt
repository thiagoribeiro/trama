package run.trama.app

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.plugins.callid.CallId
import io.ktor.server.plugins.callid.callIdMdc
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.compression.Compression
import io.ktor.server.plugins.compression.gzip
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.defaultheaders.DefaultHeaders
import io.ktor.server.plugins.forwardedheaders.ForwardedHeaders
import io.ktor.server.plugins.forwardedheaders.XForwardedHeaders
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.delete
import io.ktor.server.routing.routing
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.util.UUID
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.decodeFromString
import run.trama.config.ConfigLoader
import run.trama.runtime.RuntimeBootstrap
import run.trama.saga.ExecutionState
import run.trama.saga.RunSagaRequest
import run.trama.saga.RunStoredSagaRequest
import run.trama.saga.SagaCreateResponse
import run.trama.saga.SagaDefinition
import run.trama.saga.SagaDefinitionResponse
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaDefinitionValidator
import run.trama.saga.SagaExecution
import run.trama.saga.PayloadValue
import run.trama.saga.SagaExecutionSummary
import run.trama.saga.SagaRetryResponse
import run.trama.saga.SagaStatusResponse
import run.trama.saga.SagaStepCallResponse
import run.trama.saga.SagaStepResultResponse
import run.trama.saga.ValidationErrorResponse
import run.trama.saga.callback.CallbackReceiver
import run.trama.saga.workflow.WorkflowDefinitionValidator
import run.trama.telemetry.installRequestTracing
import org.slf4j.event.Level
import java.time.Instant
import io.ktor.server.request.receive
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.netty.EngineMain
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {
    System.setProperty("org.jooq.no-logo", "true")
    System.setProperty("org.jooq.no-tips", "true")
    EngineMain.main(args)
}

private data class ParsedDefinitionBody(
    val name: String,
    val version: String,
    val errors: List<String>,
    val definitionJson: String,
)

private fun normalizeDefinitionJson(json: Json, definitionJson: String): JsonElement {
    val raw = json.parseToJsonElement(definitionJson)
    return if (raw is JsonObject && raw.containsKey("nodes")) {
        val def = json.decodeFromJsonElement(SagaDefinitionV2.serializer(), raw)
        json.parseToJsonElement(json.encodeToString(SagaDefinitionV2.serializer(), def))
    } else {
        val def = json.decodeFromJsonElement(SagaDefinition.serializer(), raw)
        json.parseToJsonElement(json.encodeToString(SagaDefinition.serializer(), def))
    }
}

private fun parseAndValidateDefinitionBody(json: Json, body: JsonObject): ParsedDefinitionBody {
    return if (body.containsKey("nodes")) {
        val def = json.decodeFromJsonElement(SagaDefinitionV2.serializer(), body)
        ParsedDefinitionBody(
            name = def.name,
            version = def.version,
            errors = WorkflowDefinitionValidator.validate(def),
            definitionJson = json.encodeToString(SagaDefinitionV2.serializer(), def),
        )
    } else {
        val def = json.decodeFromJsonElement(SagaDefinition.serializer(), body)
        ParsedDefinitionBody(
            name = def.name,
            version = def.version,
            errors = SagaDefinitionValidator.validate(def),
            definitionJson = json.encodeToString(SagaDefinition.serializer(), def),
        )
    }
}

fun Application.module() {
    val logger = LoggerFactory.getLogger("run.trama.app.Application")
    val appConfig = ConfigLoader.load()
    val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }
    val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val bootstrap = RuntimeBootstrap(appConfig, prometheusRegistry)

    // Install Micrometer before runtime startup so Ktor can configure meter filters
    // before custom meters are registered by bootstrap components.
    if (appConfig.metrics.enabled) {
        install(MicrometerMetrics) {
            registry = prometheusRegistry
        }
    }

    if (appConfig.runtime.enabled) {
        bootstrap.start()
    }
    val repository = bootstrap.repositoryOrNull()
    installRequestTracing()

    monitor.subscribe(ApplicationStopping) {
        if (appConfig.runtime.enabled) {
            bootstrap.stop()
        }
    }

    install(DefaultHeaders) {
        header("X-Content-Type-Options", "nosniff")
        header("X-Frame-Options", "DENY")
        header("X-XSS-Protection", "0")
    }
    install(ForwardedHeaders)
    install(XForwardedHeaders)
    install(CallId) {
        generate { UUID.randomUUID().toString() }
        verify { it.isNotBlank() }
    }
    install(CallLogging) {
        level = Level.INFO
        callIdMdc("callId")
        format { call ->
            val status = call.response.status()
            val method = call.request.local.method.value
            val uri    = call.request.local.uri
            "$status $method $uri"
        }
    }
    install(Compression) {
        gzip()
    }
    install(ContentNegotiation) {
        json(json)
    }
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            val message = cause.message ?: (cause::class.simpleName ?: "internal server error")
            logger.error("Unhandled exception while processing request", cause)
            runCatching {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ValidationErrorResponse(listOf(message)),
                )
            }.onFailure {
                call.respondText(
                    text = "internal server error: $message",
                    status = HttpStatusCode.InternalServerError,
                    contentType = ContentType.Text.Plain,
                )
            }
        }
    }

    routing {
        get("/healthz") {
            call.respondText("ok")
        }
        get("/readyz") {
            val readiness = bootstrap.readiness()
            if (readiness.ok) {
                call.respondText(readiness.message)
            } else {
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    ValidationErrorResponse(listOf(readiness.message)),
                )
            }
        }
        get("/sagas") {
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val status = call.request.queryParameters["status"]
            val name = call.request.queryParameters["name"]
            val limit = call.request.queryParameters["limit"]?.toIntOrNull()?.coerceIn(1, 200) ?: 50
            val offset = call.request.queryParameters["offset"]?.toIntOrNull()?.coerceAtLeast(0) ?: 0
            val list = repo.listExecutions(status, name, limit, offset).map { rec ->
                SagaExecutionSummary(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    status = rec.status,
                    failureDescription = rec.failureDescription,
                    startedAt = rec.startedAt.toString(),
                    completedAt = rec.completedAt?.toString(),
                    updatedAt = rec.updatedAt.toString(),
                )
            }
            call.respond(list)
        }
        get("/sagas/{id}/steps") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid saga id")))
                return@get
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val steps = repo.getStepResults(id).map { rec ->
                val latencyMs = if (rec.stepStartedAt != null) {
                    java.time.Duration.between(rec.stepStartedAt, rec.createdAt).toMillis()
                } else {
                    java.time.Duration.between(rec.startedAt, rec.createdAt).toMillis()
                }
                SagaStepResultResponse(
                    id = rec.id,
                    stepIndex = rec.stepIndex,
                    stepName = rec.stepName,
                    phase = rec.phase,
                    statusCode = rec.statusCode,
                    success = rec.success,
                    responseBody = rec.responseBody?.let { json.parseToJsonElement(it) },
                    startedAt = rec.startedAt.toString(),
                    stepStartedAt = rec.stepStartedAt?.toString(),
                    createdAt = rec.createdAt.toString(),
                    latencyMs = latencyMs,
                )
            }
            call.respond(steps)
        }
        get("/sagas/{id}/steps/calls") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid saga id")))
                return@get
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val calls = repo.getStepCalls(id).map { rec ->
                SagaStepCallResponse(
                    id = rec.id,
                    stepName = rec.stepName,
                    phase = rec.phase,
                    attempt = rec.attempt,
                    requestUrl = rec.requestUrl,
                    requestBody = rec.requestBody?.let { json.parseToJsonElement(it) },
                    statusCode = rec.statusCode,
                    responseBody = rec.responseBody?.let { json.parseToJsonElement(it) },
                    error = rec.error,
                    stepStartedAt = rec.stepStartedAt.toString(),
                    createdAt = rec.createdAt.toString(),
                    latencyMs = java.time.Duration.between(rec.stepStartedAt, rec.createdAt).toMillis(),
                )
            }
            call.respond(calls)
        }
        get("/sagas/{id}") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid saga id")))
                return@get
            }
            val status = repository?.getExecutionStatus(id)
            if (status == null) {
                call.respond(HttpStatusCode.NoContent)
                return@get
            }
            call.respond(
                SagaStatusResponse(
                    id = status.id.toString(),
                    name = status.name,
                    version = status.version,
                    status = status.status,
                    failureDescription = status.failureDescription,
                    callbackWarning = status.callbackWarning,
                    startedAt = status.startedAt.toString(),
                    completedAt = status.completedAt?.toString(),
                    updatedAt = status.updatedAt.toString(),
                )
            )
        }
        post("/sagas/{id}/retry") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid saga id")))
                return@post
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@post
            }
            val retryData = repo.getExecutionForRetry(id)
            if (retryData == null) {
                call.respond(HttpStatusCode.NoContent)
                return@post
            }
            val definitionJson = retryData.definitionJson
            if (definitionJson.isNullOrBlank()) {
                call.respond(HttpStatusCode.Conflict, ValidationErrorResponse(listOf("saga definition not stored")))
                return@post
            }
            val isV2Definition = runCatching {
                json.parseToJsonElement(definitionJson).jsonObject.containsKey("nodes")
            }.getOrDefault(false)
            if (isV2Definition) {
                call.respond(HttpStatusCode.UnprocessableEntity, ValidationErrorResponse(listOf("retry of v2 saga definitions is not yet supported")))
                return@post
            }
            val definition = json.decodeFromString(SagaDefinition.serializer(), definitionJson)
            val startIndex = retryData.failedStepIndex ?: 0
            val activeNodeId = definition.steps.getOrElse(startIndex) { definition.steps.last() }.name
            val completedNodes = definition.steps.take(startIndex).map { it.name }
            val compensationStack = definition.steps.take(startIndex).reversed().map { it.name }
            val execution = SagaExecution(
                definition = definition,
                id = retryData.id,
                startedAt = retryData.startedAt,
                currentStepIndex = startIndex,
                state = ExecutionState.InProgress(
                    activeNodeId = activeNodeId,
                    completedNodes = completedNodes,
                    compensationStack = compensationStack,
                ),
                payload = emptyMap(),
            )
            repo.markRetrying(id)
            bootstrap.enqueueRetry(execution)
            call.respond(HttpStatusCode.Accepted, SagaRetryResponse(id.toString(), "REQUEUED"))
        }
        post("/sagas/run") {
            val rawBody = call.receive<JsonObject>()
            val definitionObj = rawBody["definition"] as? JsonObject
            if (definitionObj != null && definitionObj.containsKey("nodes")) {
                // ── V2 node-graph inline run ───────────────────────────────────────
                val def = runCatching {
                    json.decodeFromJsonElement(SagaDefinitionV2.serializer(), definitionObj)
                }.getOrElse {
                    call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid v2 definition: ${it.message}")))
                    return@post
                }
                val errors = WorkflowDefinitionValidator.validate(def)
                if (errors.isNotEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                    return@post
                }
                if (bootstrap.repositoryOrNull() == null) {
                    call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                    return@post
                }
                val rawPayload = rawBody["payload"]
                val payload: Map<String, JsonElement> = if (rawPayload is JsonObject) rawPayload else emptyMap()
                // Stub v1 definition carries name/version for logging; executor uses definitionV2
                val stub = SagaDefinition(
                    name = def.name,
                    version = def.version,
                    failureHandling = def.failureHandling,
                    steps = emptyList(),
                )
                val execution = SagaExecution(
                    definition = stub,
                    definitionV2 = def,
                    id = UUID.randomUUID(),
                    startedAt = Instant.now(),
                    currentStepIndex = 0,
                    state = ExecutionState.InProgress(activeNodeId = def.entrypoint),
                    payload = payload.mapValues { PayloadValue(it.value) },
                )
                bootstrap.enqueueRetry(execution)
                call.respond(SagaCreateResponse(execution.id.toString()))
            } else {
                // ── V1 steps-based inline run (existing logic) ─────────────────────
                val req = runCatching {
                    json.decodeFromJsonElement(RunSagaRequest.serializer(), rawBody)
                }.getOrElse {
                    call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid request: ${it.message}")))
                    return@post
                }
                val errors = SagaDefinitionValidator.validate(req.definition)
                if (errors.isNotEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                    return@post
                }
                val execution = SagaExecution(
                    definition = req.definition,
                    id = UUID.randomUUID(),
                    startedAt = Instant.now(),
                    currentStepIndex = 0,
                    state = ExecutionState.InProgress(
                        activeNodeId = req.definition.steps.first().name,
                    ),
                    payload = req.payload.mapValues { PayloadValue(it.value) },
                )
                if (bootstrap.repositoryOrNull() == null) {
                    call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                    return@post
                }
                bootstrap.enqueueRetry(execution)
                call.respond(SagaCreateResponse(execution.id.toString()))
            }
        }
        post("/sagas/definitions") {
            val body = call.receive<JsonObject>()
            val (name, version, errors, definitionJson) = parseAndValidateDefinitionBody(json, body)
            if (errors.isNotEmpty()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                return@post
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@post
            }
            val defId = UUID.randomUUID()
            val inserted = repo.insertDefinition(defId, name, version, definitionJson)
            if (!inserted) {
                call.respond(HttpStatusCode.Conflict, ValidationErrorResponse(listOf("definition already exists")))
                return@post
            }
            val stored = repo.getDefinition(defId)!!
            call.respond(
                SagaDefinitionResponse(
                    id = stored.id.toString(),
                    name = stored.name,
                    version = stored.version,
                    definition = normalizeDefinitionJson(json, stored.definitionJson),
                    createdAt = stored.createdAt.toString(),
                    updatedAt = stored.updatedAt.toString(),
                )
            )
        }
        get("/sagas/definitions") {
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val limit = call.request.queryParameters["limit"]?.toIntOrNull()?.coerceIn(1, 200) ?: 50
            val offset = call.request.queryParameters["offset"]?.toIntOrNull()?.coerceAtLeast(0) ?: 0
            val list = repo.listDefinitions(limit, offset).map { rec ->
                SagaDefinitionResponse(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    definition = normalizeDefinitionJson(json, rec.definitionJson),
                    createdAt = rec.createdAt.toString(),
                    updatedAt = rec.updatedAt.toString(),
                )
            }
            call.respond(list)
        }
        get("/sagas/definitions/{id}") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid definition id")))
                return@get
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val rec = repo.getDefinition(id)
            if (rec == null) {
                call.respond(HttpStatusCode.NoContent)
                return@get
            }
            call.respond(
                SagaDefinitionResponse(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    definition = normalizeDefinitionJson(json, rec.definitionJson),
                    createdAt = rec.createdAt.toString(),
                    updatedAt = rec.updatedAt.toString(),
                )
            )
        }
        put("/sagas/definitions/{id}") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid definition id")))
                return@put
            }
            val body = call.receive<JsonObject>()
            val (name, version, errors, definitionJson) = parseAndValidateDefinitionBody(json, body)
            if (errors.isNotEmpty()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                return@put
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@put
            }
            val inserted = repo.insertDefinition(id, name, version, definitionJson)
            if (!inserted) {
                call.respond(HttpStatusCode.Conflict, ValidationErrorResponse(listOf("definition already exists")))
                return@put
            }
            val stored = repo.getDefinition(id)!!
            call.respond(
                SagaDefinitionResponse(
                    id = stored.id.toString(),
                    name = stored.name,
                    version = stored.version,
                    definition = normalizeDefinitionJson(json, stored.definitionJson),
                    createdAt = stored.createdAt.toString(),
                    updatedAt = stored.updatedAt.toString(),
                )
            )
        }
        delete("/sagas/definitions/{id}") {
            val idParam = call.parameters["id"]
            val id = runCatching { UUID.fromString(idParam) }.getOrNull()
            if (id == null) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid definition id")))
                return@delete
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@delete
            }
            val deleted = repo.deleteDefinition(id)
            if (!deleted) {
                call.respond(HttpStatusCode.NoContent)
                return@delete
            }
            call.respond(HttpStatusCode.NoContent)
        }
        get("/sagas/definitions/{name}/{version}") {
            val name = call.parameters["name"]?.trim().orEmpty()
            val version = call.parameters["version"]?.trim().orEmpty()
            if (name.isBlank() || version.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid definition name/version")))
                return@get
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@get
            }
            val rec = repo.getDefinitionByNameVersion(name, version)
            if (rec == null) {
                call.respond(HttpStatusCode.NoContent)
                return@get
            }
            call.respond(
                SagaDefinitionResponse(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    definition = normalizeDefinitionJson(json, rec.definitionJson),
                    createdAt = rec.createdAt.toString(),
                    updatedAt = rec.updatedAt.toString(),
                )
            )
        }
        post("/sagas/definitions/{name}/{version}/run") {
            val name = call.parameters["name"]?.trim().orEmpty()
            val version = call.parameters["version"]?.trim().orEmpty()
            if (name.isBlank() || version.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid definition name/version")))
                return@post
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@post
            }
            val req = call.receive<RunStoredSagaRequest>()
            val rec = repo.getDefinitionByNameVersion(name, version)
            if (rec == null) {
                call.respond(HttpStatusCode.NoContent)
                return@post
            }
            val isV2StoredDef = runCatching {
                json.parseToJsonElement(rec.definitionJson).jsonObject.containsKey("nodes")
            }.getOrDefault(false)
            if (isV2StoredDef) {
                call.respond(HttpStatusCode.UnprocessableEntity, ValidationErrorResponse(listOf("running stored v2 saga definitions is not yet supported")))
                return@post
            }
            val definition = json.decodeFromString(SagaDefinition.serializer(), rec.definitionJson)
            val errors = SagaDefinitionValidator.validate(definition)
            if (errors.isNotEmpty()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                return@post
            }
            val execution = SagaExecution(
                definition = definition,
                id = UUID.randomUUID(),
                startedAt = Instant.now(),
                currentStepIndex = 0,
                state = ExecutionState.InProgress(
                    activeNodeId = definition.steps.first().name,
                ),
                payload = req.payload.mapValues { PayloadValue(it.value) },
            )
            bootstrap.enqueueRetry(execution)
            call.respond(SagaCreateResponse(execution.id.toString()))
        }

        post("/sagas/{executionId}/node/{nodeId}/callback") {
            val executionIdParam = call.parameters["executionId"]
            val nodeId = call.parameters["nodeId"]?.trim().orEmpty()
            val executionId = runCatching { UUID.fromString(executionIdParam) }.getOrNull()
            if (executionId == null || nodeId.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("invalid executionId or nodeId")))
                return@post
            }
            val receiver = bootstrap.callbackReceiverOrNull()
            if (receiver == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("async callbacks not configured")))
                return@post
            }
            val token = call.request.headers["X-Callback-Token"].orEmpty()
            if (token.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(listOf("missing X-Callback-Token header")))
                return@post
            }
            val rawBody = runCatching { call.receive<String>() }.getOrDefault("")
            when (val result = receiver.receive(executionId, nodeId, token, rawBody)) {
                is CallbackReceiver.CallbackResult.Accepted ->
                    call.respond(HttpStatusCode.Accepted)
                is CallbackReceiver.CallbackResult.Rejected ->
                    call.respond(HttpStatusCode(result.httpStatus, result.message), ValidationErrorResponse(listOf(result.message)))
            }
        }

        if (appConfig.metrics.enabled) {
            get("/metrics") {
                call.respondText(prometheusRegistry.scrape(), ContentType.parse("text/plain; version=0.0.4"))
            }
        }
    }
}
