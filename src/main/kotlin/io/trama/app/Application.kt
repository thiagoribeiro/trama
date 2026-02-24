package io.trama.app

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
import kotlinx.serialization.decodeFromString
import io.trama.config.ConfigLoader
import io.trama.runtime.RuntimeBootstrap
import io.trama.saga.ExecutionPhase
import io.trama.saga.ExecutionState
import io.trama.saga.RunSagaRequest
import io.trama.saga.RunStoredSagaRequest
import io.trama.saga.SagaCreateResponse
import io.trama.saga.SagaDefinition
import io.trama.saga.SagaDefinitionCreateRequest
import io.trama.saga.SagaDefinitionResponse
import io.trama.saga.SagaDefinitionValidator
import io.trama.saga.SagaExecution
import io.trama.saga.SagaRetryResponse
import io.trama.saga.SagaStatusResponse
import io.trama.saga.ValidationErrorResponse
import io.trama.telemetry.installRequestTracing
import org.slf4j.event.Level
import java.time.Instant
import io.ktor.server.request.receive
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.netty.EngineMain

fun main(args: Array<String>) {
    EngineMain.main(args)
}

fun Application.module() {
    val appConfig = ConfigLoader.load()
    val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
        encodeDefaults = true
    }
    val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val bootstrap = RuntimeBootstrap(appConfig, prometheusRegistry)
    if (appConfig.runtime.enabled) {
        bootstrap.start()
    }
    val repository = bootstrap.repositoryOrNull()
    installRequestTracing()

    monitor.subscribe(ApplicationStopped) {
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
    }
    install(Compression) {
        gzip()
    }
    install(ContentNegotiation) {
        json(json)
    }
    install(StatusPages) {
        exception<Throwable> { call, _ ->
            call.respond(HttpStatusCode.InternalServerError)
        }
    }

    if (appConfig.metrics.enabled) {
        install(MicrometerMetrics) {
            registry = prometheusRegistry
        }
    }

    routing {
        get("/healthz") {
            call.respondText("ok")
        }
        get("/readyz") {
            call.respondText("ready")
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
            val definition = json.decodeFromString(SagaDefinition.serializer(), definitionJson)
            val startIndex = retryData.failedStepIndex ?: 0
            val execution = SagaExecution(
                definition = definition,
                id = retryData.id,
                startedAt = retryData.startedAt,
                currentStepIndex = startIndex,
                state = ExecutionState.InProgress(ExecutionPhase.UP),
                payload = emptyMap(),
            )
            bootstrap.enqueueRetry(execution)
            repo.markRetrying(id)
            call.respond(HttpStatusCode.Accepted, SagaRetryResponse(id.toString(), "REQUEUED"))
        }
        post("/sagas/run") {
            val req = call.receive<RunSagaRequest>()
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
                state = ExecutionState.InProgress(ExecutionPhase.UP),
                payload = req.payload,
            )
            if (bootstrap.repositoryOrNull() == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@post
            }
            bootstrap.enqueueRetry(execution)
            call.respond(SagaCreateResponse(execution.id.toString()))
        }
        post("/sagas/definitions") {
            val req = call.receive<SagaDefinitionCreateRequest>()
            val definition = SagaDefinition(
                name = req.name,
                version = req.version,
                failureHandling = req.failureHandling,
                steps = req.steps,
                onSuccessCallback = req.onSuccessCallback,
                onFailureCallback = req.onFailureCallback,
            )
            val errors = SagaDefinitionValidator.validate(definition)
            if (errors.isNotEmpty()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                return@post
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@post
            }
            val id = UUID.randomUUID()
            val definitionJson = json.encodeToString(SagaDefinition.serializer(), definition)
            val inserted = repo.insertDefinition(id, definition.name, definition.version, definitionJson)
            if (!inserted) {
                call.respond(HttpStatusCode.Conflict, ValidationErrorResponse(listOf("definition already exists")))
                return@post
            }
            val stored = repo.getDefinition(id)!!
            call.respond(
                SagaDefinitionResponse(
                    id = stored.id.toString(),
                    name = stored.name,
                    version = stored.version,
                    definition = definition,
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
            val list = repo.listDefinitions().map { rec ->
                val definition = json.decodeFromString(SagaDefinition.serializer(), rec.definitionJson)
                SagaDefinitionResponse(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    definition = definition,
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
            val definition = json.decodeFromString(SagaDefinition.serializer(), rec.definitionJson)
            call.respond(
                SagaDefinitionResponse(
                    id = rec.id.toString(),
                    name = rec.name,
                    version = rec.version,
                    definition = definition,
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
            val req = call.receive<SagaDefinitionCreateRequest>()
            val definition = SagaDefinition(
                name = req.name,
                version = req.version,
                failureHandling = req.failureHandling,
                steps = req.steps,
                onSuccessCallback = req.onSuccessCallback,
                onFailureCallback = req.onFailureCallback,
            )
            val errors = SagaDefinitionValidator.validate(definition)
            if (errors.isNotEmpty()) {
                call.respond(HttpStatusCode.BadRequest, ValidationErrorResponse(errors))
                return@put
            }
            val repo = repository
            if (repo == null) {
                call.respond(HttpStatusCode.ServiceUnavailable, ValidationErrorResponse(listOf("runtime disabled")))
                return@put
            }
            val definitionJson = json.encodeToString(SagaDefinition.serializer(), definition)
            val inserted = repo.insertDefinition(id, definition.name, definition.version, definitionJson)
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
                    definition = definition,
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
                state = ExecutionState.InProgress(ExecutionPhase.UP),
                payload = req.payload,
            )
            bootstrap.enqueueRetry(execution)
            call.respond(SagaCreateResponse(execution.id.toString()))
        }
        post("/sagas") {
            val definition = call.receive<SagaDefinition>()
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
                state = ExecutionState.InProgress(ExecutionPhase.UP),
            )
            call.respond(SagaCreateResponse(execution.id.toString()))
        }
        if (appConfig.metrics.enabled) {
            get("/metrics") {
                call.respondText(prometheusRegistry.scrape(), ContentType.parse("text/plain; version=0.0.4"))
            }
        }
    }
}
