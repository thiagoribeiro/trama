package run.trama.saga.redis

import io.lettuce.core.ScriptOutputType

/**
 * Writes to the due index ([RedisShardKeyspace.queueDueKey]): one sorted set whose members are
 * shard ids scored by the earliest time the shard has work. Enqueuers mark a shard when they add
 * to it; claimers remove the marks of the shards they are about to visit and re-mark whatever the
 * claim leaves behind. A single key, so its script is valid on Redis Cluster.
 */
internal object DueIndex {
    /** Lowers each shard's mark to the given time, keeping an earlier one. ARGV = shard, time, ... */
    private val markScript = LuaScript("""
        for i = 1, #ARGV, 2 do
            local current = redis.call('ZSCORE', KEYS[1], ARGV[i])
            if (not current) or tonumber(current) > tonumber(ARGV[i + 1]) then
                redis.call('ZADD', KEYS[1], ARGV[i + 1], ARGV[i])
            end
        end
        return 1
    """.trimIndent())

    /** [shardTimePairs] alternates shard id and epoch-millis time, as strings. */
    suspend fun mark(commands: RedisBinaryCommands, dueKey: ByteArray, shardTimePairs: List<String>) {
        if (shardTimePairs.isEmpty()) return
        val args = shardTimePairs.map { it.toByteArray() }.toTypedArray()
        markScript.run<Long>(commands, ScriptOutputType.INTEGER, arrayOf(dueKey), *args)
    }
}
