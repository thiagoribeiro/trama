package run.trama.telemetry

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import io.opentelemetry.context.Scope
import io.opentelemetry.context.propagation.TextMapSetter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import run.trama.config.TelemetryConfig
import org.slf4j.MDC

object Tracing {
    @Volatile
    private var sdk: OpenTelemetrySdk? = null

    fun initialize(config: TelemetryConfig) {
        if (!config.enabled || sdk != null) return

        val exporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint(config.otlpEndpoint)
            .build()

        val resource = Resource.getDefault().merge(
            Resource.create(
                Attributes.of(ResourceAttributes.SERVICE_NAME, config.serviceName)
            )
        )

        val tracerProvider = SdkTracerProvider.builder()
            .setResource(resource)
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build()

        val openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .build()

        GlobalOpenTelemetry.set(openTelemetry)
        sdk = openTelemetry
    }

    fun tracer(name: String = "saga"): Tracer =
        GlobalOpenTelemetry.getTracer(name)

    fun shutdown() {
        sdk?.sdkTracerProvider?.shutdown()
        sdk = null
    }

    suspend fun <T> withSpan(
        tracer: Tracer,
        name: String,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Map<String, String> = emptyMap(),
        block: suspend (Span) -> T,
    ): T {
        val spanBuilder = tracer.spanBuilder(name).setSpanKind(kind)
        attributes.forEach { (k, v) ->
            spanBuilder.setAttribute(AttributeKey.stringKey(k), v)
        }
        val span = spanBuilder.startSpan()
        val scope: Scope = span.makeCurrent()
        try {
            return block(span)
        } catch (ex: Exception) {
            span.recordException(ex)
            span.setStatus(StatusCode.ERROR)
            throw ex
        } finally {
            scope.close()
            span.end()
        }
    }

    fun withTraceMdc(span: Span, sagaId: String? = null, block: () -> Unit) {
        val traceId = span.spanContext.traceId
        val spanId = span.spanContext.spanId
        MDC.put("traceId", traceId)
        MDC.put("spanId", spanId)
        if (sagaId != null) {
            MDC.put("sagaId", sagaId)
        }
        try {
            block()
        } finally {
            MDC.remove("traceId")
            MDC.remove("spanId")
            MDC.remove("sagaId")
        }
    }

    suspend fun <T> withTraceMdcSuspend(span: Span, sagaId: String? = null, block: suspend () -> T): T {
        val traceId = span.spanContext.traceId
        val spanId = span.spanContext.spanId
        MDC.put("traceId", traceId)
        MDC.put("spanId", spanId)
        if (sagaId != null) {
            MDC.put("sagaId", sagaId)
        }
        return try {
            block()
        } finally {
            MDC.remove("traceId")
            MDC.remove("spanId")
            MDC.remove("sagaId")
        }
    }

    fun injectHeaders(setter: (String, String) -> Unit) {
        val propagator = GlobalOpenTelemetry.getPropagators().textMapPropagator
        propagator.inject(Context.current(), setter, headerSetter)
    }

    private val headerSetter = TextMapSetter<(String, String) -> Unit> { carrier, key, value ->
        carrier?.invoke(key, value)
    }
}
