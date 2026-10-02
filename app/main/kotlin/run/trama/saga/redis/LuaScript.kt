package run.trama.saga.redis

import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import java.security.MessageDigest

/**
 * A Lua script run by digest (EVALSHA), so polls don't resend its source. The digest is computed
 * locally, so nothing has to be loaded at startup. When Redis doesn't know the script (a restart,
 * failover or SCRIPT FLUSH emptied its cache) the call falls back to EVAL once, which also caches
 * it again: scripts can never get stuck failing with NOSCRIPT.
 */
class LuaScript(source: String) {
    private val body: ByteArray = source.toByteArray()
    val sha: String = MessageDigest.getInstance("SHA-1").digest(body).joinToString("") { "%02x".format(it) }

    suspend fun <T> run(
        commands: RedisBinaryCommands,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T? = try {
        commands.evalsha(sha, outputType, keys, *values)
    } catch (_: RedisNoScriptException) {
        commands.eval(body, outputType, keys, *values)
    }
}
