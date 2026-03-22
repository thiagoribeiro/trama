package run.trama.saga.callback

import java.util.UUID

class CallbackUrlFactory(private val baseUrl: String) {
    fun buildUrl(executionId: UUID, nodeId: String): String =
        "${baseUrl.trimEnd('/')}/sagas/$executionId/node/$nodeId/callback"
}
