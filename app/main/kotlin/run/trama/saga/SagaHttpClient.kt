package run.trama.saga

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import run.trama.config.HttpConfig

interface HttpClientProvider {
    val client: HttpClient
}

/**
 * Client for workflow HTTP calls, on OkHttp: it keeps a pool of keep-alive connections per host.
 * Ktor's CIO engine (used before) opened a new connection for every request, paying a TCP
 * handshake on each node and leaving one TIME_WAIT socket behind per call. Measured against the
 * JDK engine too, OkHttp used the least CPU per call.
 */
class SagaHttpClient(
    config: HttpConfig,
) : AutoCloseable, HttpClientProvider {
    override val client: HttpClient = HttpClient(OkHttp) {
        engine {
            config {
                // OkHttp's defaults cap concurrent calls at 5 per host and keep 5 idle connections.
                dispatcher(okhttp3.Dispatcher().apply { maxRequests = 1024; maxRequestsPerHost = 1024 })
                connectionPool(okhttp3.ConnectionPool(256, 5, java.util.concurrent.TimeUnit.MINUTES))
            }
        }
        install(HttpTimeout) {
            connectTimeoutMillis = config.connectTimeoutMillis
            requestTimeoutMillis = config.requestTimeoutMillis
            socketTimeoutMillis = config.socketTimeoutMillis
        }
    }

    override fun close() {
        client.close()
    }
}
