@file:OptIn(io.lettuce.core.ExperimentalLettuceCoroutinesApi::class)

package run.trama.saga.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisCoroutinesCommandsImpl
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.support.ConnectionPoolSupport
import run.trama.config.RedisConfig
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

interface RedisCommandsProvider {
    suspend fun <T> withCommands(block: suspend (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T): T
}

class RedisClientProvider(
    private val config: RedisConfig,
) : RedisCommandsProvider, AutoCloseable {
    private val client: RedisClient
    private val pool: GenericObjectPool<StatefulRedisConnection<ByteArray, ByteArray>>

    init {
        val redisUri: RedisURI = RedisURI.create(config.url)
        val redisClient = RedisClient.create(redisUri)
        val poolConfig: GenericObjectPoolConfig<StatefulRedisConnection<ByteArray, ByteArray>> =
            GenericObjectPoolConfig<StatefulRedisConnection<ByteArray, ByteArray>>().apply {
                maxTotal = config.pool.maxTotal
                maxIdle = config.pool.maxIdle
                minIdle = config.pool.minIdle
                testOnBorrow = config.pool.testOnBorrow
                testWhileIdle = config.pool.testWhileIdle
            }

        val redisPool: GenericObjectPool<StatefulRedisConnection<ByteArray, ByteArray>> =
            ConnectionPoolSupport.createGenericObjectPool(
                { redisClient.connect(ByteArrayCodec.INSTANCE) },
                poolConfig,
            )

        client = redisClient
        pool = redisPool

        val connection = pool.borrowObject()
        pool.returnObject(connection)
    }

    override suspend fun <T> withCommands(
        block: suspend (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T
    ): T {
        val connection = pool.borrowObject()
        try {
            val commands = RedisCoroutinesCommandsImpl(connection.reactive())
            return block(commands)
        } finally {
            pool.returnObject(connection)
        }
    }

    override fun close() {
        pool.close()
        client.shutdown()
    }
}
