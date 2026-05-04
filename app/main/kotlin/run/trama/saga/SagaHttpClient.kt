package run.trama.saga

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import run.trama.config.HttpConfig

interface HttpClientProvider {
    val client: HttpClient
}

class SagaHttpClient(
    config: HttpConfig,
) : AutoCloseable, HttpClientProvider {
    override val client: HttpClient = HttpClient(CIO) {
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
