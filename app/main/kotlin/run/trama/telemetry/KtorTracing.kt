package run.trama.telemetry

import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.util.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode

private val spanKey = AttributeKey<Span>("otel-span")

fun Application.installRequestTracing() {
    val tracer = Tracing.tracer("http-server")
    intercept(ApplicationCallPipeline.Setup) {
        val currentCall = call
        val span = tracer.spanBuilder("${currentCall.request.httpMethod.value} ${currentCall.request.path()}")
            .setSpanKind(SpanKind.SERVER)
            .startSpan()
        currentCall.attributes.put(spanKey, span)
        val scope = span.makeCurrent()
        span.setAttribute("http.method", currentCall.request.httpMethod.value)
        span.setAttribute("http.route", currentCall.request.path())
        try {
            Tracing.withTraceMdcSuspend<Unit>(span) { proceed() }
        } catch (ex: Exception) {
            span.recordException(ex)
            span.setStatus(StatusCode.ERROR)
            throw ex
        } finally {
            currentCall.response.status()?.let { status ->
                span.setAttribute("http.status_code", status.value.toLong())
                if (status.value >= 500) span.setStatus(StatusCode.ERROR)
            }
            scope.close()
            span.end()
        }
    }
}
