@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.RedisClient
import io.lettuce.core.SetArgs
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines as standaloneCoroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.coroutines as clusterCoroutines
import io.lettuce.core.cluster.api.coroutines.RedisClusterCoroutinesCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.support.ConnectionPoolSupport
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.flow.toList
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import run.trama.config.RedisConfig
import run.trama.config.RedisTopology

interface RedisBinaryCommands {
    suspend fun zadd(key: ByteArray, score: Double, member: ByteArray): Long?
    suspend fun zrem(key: ByteArray, member: ByteArray): Long?
    suspend fun zremrangebyscore(key: ByteArray, min: Double, max: Double): Long?
    suspend fun zrangebyscore(key: ByteArray, min: Double, max: Double, limit: Int? = null): List<ByteArray>
    suspend fun <T> eval(
        script: ByteArray,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T?
    /** Loads a Lua script into Redis script cache. Returns the SHA1 digest. */
    suspend fun scriptLoad(script: ByteArray): String
    /** Executes a cached Lua script by its SHA1 digest (EVALSHA). */
    suspend fun <T> evalsha(
        digest: String,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T?
    suspend fun get(key: ByteArray): ByteArray?
    suspend fun incr(key: ByteArray): Long?
    suspend fun pexpire(key: ByteArray, milliseconds: Long): Boolean?
    suspend fun psetex(key: ByteArray, milliseconds: Long, value: ByteArray): String?
    suspend fun setex(key: ByteArray, seconds: Long, value: ByteArray): String?
    suspend fun expire(key: ByteArray, seconds: Long): Boolean?
    suspend fun set(key: ByteArray, value: ByteArray): String?
    /** SET key value NX EX ttlSeconds — returns true if key was set (fresh), false if already existed. */
    suspend fun setNx(key: ByteArray, value: ByteArray, ttlSeconds: Long): Boolean
    suspend fun del(vararg keys: ByteArray): Long?
    suspend fun lpush(key: ByteArray, value: ByteArray): Long?
    suspend fun lrange(key: ByteArray, start: Long, stop: Long): List<ByteArray>
    suspend fun ping(): String
}

interface RedisCommandsProvider {
    suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T
}

class RedisClientProvider(
    private val config: RedisConfig,
) : RedisCommandsProvider, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val backend: RedisBackend =
        when (config.topology) {
            RedisTopology.STANDALONE -> StandaloneBackend(config)
            RedisTopology.CLUSTER -> ClusterBackend(config)
        }

    override suspend fun <T> withCommands(
        block: suspend (RedisBinaryCommands) -> T
    ): T = backend.withCommands(block)

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            backend.close()
        }
    }
}

private interface RedisBackend : AutoCloseable {
    suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T
}

private class StandaloneBackend(
    config: RedisConfig,
) : RedisBackend {
    private val client: RedisClient
    private val pool: GenericObjectPool<StatefulRedisConnection<ByteArray, ByteArray>>

    init {
        val redisUri = RedisURI.create(config.url)
        val redisClient = RedisClient.create(redisUri)
        val poolConfig = GenericObjectPoolConfig<StatefulRedisConnection<ByteArray, ByteArray>>().apply {
            maxTotal = config.pool.maxTotal
            maxIdle = config.pool.maxIdle
            minIdle = config.pool.minIdle
            testOnBorrow = config.pool.testOnBorrow
            testWhileIdle = config.pool.testWhileIdle
        }
        val redisPool =
            ConnectionPoolSupport.createGenericObjectPool(
                { redisClient.connect(ByteArrayCodec.INSTANCE) },
                poolConfig,
            )

        client = redisClient
        pool = redisPool

        val connection = pool.borrowObject()
        pool.returnObject(connection)
    }

    override suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T {
        val connection = pool.borrowObject()
        try {
            val commands = connection.standaloneCoroutines()
            return block(StandaloneBinaryCommands(commands))
        } finally {
            pool.returnObject(connection)
        }
    }

    override fun close() {
        pool.close()
        client.shutdown()
    }
}

private class ClusterBackend(
    config: RedisConfig,
) : RedisBackend {
    private val client: RedisClusterClient
    private val pool: GenericObjectPool<StatefulRedisClusterConnection<ByteArray, ByteArray>>

    init {
        val nodes = if (config.cluster.nodes.isNotEmpty()) {
            config.cluster.nodes
        } else {
            listOf(config.url)
        }
        val uris = nodes.map(RedisURI::create)
        val clusterClient = RedisClusterClient.create(uris)
        val poolConfig = GenericObjectPoolConfig<StatefulRedisClusterConnection<ByteArray, ByteArray>>().apply {
            maxTotal = config.pool.maxTotal
            maxIdle = config.pool.maxIdle
            minIdle = config.pool.minIdle
            testOnBorrow = config.pool.testOnBorrow
            testWhileIdle = config.pool.testWhileIdle
        }
        val clusterPool = ConnectionPoolSupport.createGenericObjectPool(
            { clusterClient.connect(ByteArrayCodec.INSTANCE) },
            poolConfig,
        )

        client = clusterClient
        pool = clusterPool

        val connection = pool.borrowObject()
        pool.returnObject(connection)
    }

    override suspend fun <T> withCommands(block: suspend (RedisBinaryCommands) -> T): T {
        val connection = pool.borrowObject()
        try {
            val commands = connection.clusterCoroutines()
            return block(ClusterBinaryCommands(commands))
        } finally {
            pool.returnObject(connection)
        }
    }

    override fun close() {
        pool.close()
        client.shutdown()
    }
}

private class StandaloneBinaryCommands(
    private val delegate: RedisCoroutinesCommands<ByteArray, ByteArray>,
) : RedisBinaryCommands {
    override suspend fun zadd(key: ByteArray, score: Double, member: ByteArray): Long? =
        delegate.zadd(key, score, member)

    override suspend fun zrem(key: ByteArray, member: ByteArray): Long? =
        delegate.zrem(key, member)

    override suspend fun zremrangebyscore(key: ByteArray, min: Double, max: Double): Long? =
        delegate.zremrangebyscore(key, Range.create(min, max))

    override suspend fun zrangebyscore(
        key: ByteArray,
        min: Double,
        max: Double,
        limit: Int?,
    ): List<ByteArray> {
        val range = Range.create(min, max)
        return if (limit == null) {
            delegate.zrangebyscore(key, range).toList()
        } else {
            delegate.zrangebyscore(key, range, Limit.create(0, limit.toLong())).toList()
        }
    }

    override suspend fun <T> eval(
        script: ByteArray,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T? = delegate.eval(script, outputType, keys, *values)

    override suspend fun scriptLoad(script: ByteArray): String =
        delegate.scriptLoad(script) ?: error("SCRIPT LOAD returned null")

    override suspend fun <T> evalsha(
        digest: String,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T? = delegate.evalsha(digest, outputType, keys, *values)

    override suspend fun get(key: ByteArray): ByteArray? = delegate.get(key)

    override suspend fun incr(key: ByteArray): Long? = delegate.incr(key)

    override suspend fun pexpire(key: ByteArray, milliseconds: Long): Boolean? =
        delegate.pexpire(key, milliseconds)

    override suspend fun psetex(key: ByteArray, milliseconds: Long, value: ByteArray): String? =
        delegate.psetex(key, milliseconds, value)

    override suspend fun setex(key: ByteArray, seconds: Long, value: ByteArray): String? =
        delegate.setex(key, seconds, value)

    override suspend fun expire(key: ByteArray, seconds: Long): Boolean? =
        delegate.expire(key, seconds)

    override suspend fun set(key: ByteArray, value: ByteArray): String? =
        delegate.set(key, value)

    override suspend fun setNx(key: ByteArray, value: ByteArray, ttlSeconds: Long): Boolean =
        delegate.set(key, value, SetArgs.Builder.nx().ex(ttlSeconds)) != null

    override suspend fun del(vararg keys: ByteArray): Long? =
        delegate.del(*keys)

    override suspend fun lpush(key: ByteArray, value: ByteArray): Long? =
        delegate.lpush(key, value)

    override suspend fun lrange(key: ByteArray, start: Long, stop: Long): List<ByteArray> =
        delegate.lrange(key, start, stop)

    override suspend fun ping(): String =
        delegate.ping()
}

private class ClusterBinaryCommands(
    private val delegate: RedisClusterCoroutinesCommands<ByteArray, ByteArray>,
) : RedisBinaryCommands {
    override suspend fun zadd(key: ByteArray, score: Double, member: ByteArray): Long? =
        delegate.zadd(key, score, member)

    override suspend fun zrem(key: ByteArray, member: ByteArray): Long? =
        delegate.zrem(key, member)

    override suspend fun zremrangebyscore(key: ByteArray, min: Double, max: Double): Long? =
        delegate.zremrangebyscore(key, Range.create(min, max))

    override suspend fun zrangebyscore(
        key: ByteArray,
        min: Double,
        max: Double,
        limit: Int?,
    ): List<ByteArray> {
        val range = Range.create(min, max)
        return if (limit == null) {
            delegate.zrangebyscore(key, range).toList()
        } else {
            delegate.zrangebyscore(key, range, Limit.create(0, limit.toLong())).toList()
        }
    }

    override suspend fun <T> eval(
        script: ByteArray,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T? = delegate.eval(script, outputType, keys, *values)

    override suspend fun scriptLoad(script: ByteArray): String =
        delegate.scriptLoad(script) ?: error("SCRIPT LOAD returned null")

    override suspend fun <T> evalsha(
        digest: String,
        outputType: ScriptOutputType,
        keys: Array<ByteArray>,
        vararg values: ByteArray,
    ): T? = delegate.evalsha(digest, outputType, keys, *values)

    override suspend fun get(key: ByteArray): ByteArray? = delegate.get(key)

    override suspend fun incr(key: ByteArray): Long? = delegate.incr(key)

    override suspend fun pexpire(key: ByteArray, milliseconds: Long): Boolean? =
        delegate.pexpire(key, milliseconds)

    override suspend fun psetex(key: ByteArray, milliseconds: Long, value: ByteArray): String? =
        delegate.psetex(key, milliseconds, value)

    override suspend fun setex(key: ByteArray, seconds: Long, value: ByteArray): String? =
        delegate.setex(key, seconds, value)

    override suspend fun expire(key: ByteArray, seconds: Long): Boolean? =
        delegate.expire(key, seconds)

    override suspend fun set(key: ByteArray, value: ByteArray): String? =
        delegate.set(key, value)

    override suspend fun setNx(key: ByteArray, value: ByteArray, ttlSeconds: Long): Boolean =
        delegate.set(key, value, SetArgs.Builder.nx().ex(ttlSeconds)) != null

    override suspend fun del(vararg keys: ByteArray): Long? =
        delegate.del(*keys)

    override suspend fun lpush(key: ByteArray, value: ByteArray): Long? =
        delegate.lpush(key, value)

    override suspend fun lrange(key: ByteArray, start: Long, stop: Long): List<ByteArray> =
        delegate.lrange(key, start, stop)

    override suspend fun ping(): String =
        delegate.ping()
}
