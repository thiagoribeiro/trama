package io.trama.runtime

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.trama.config.AppConfig
import io.trama.config.RuntimeStore
import io.trama.saga.DefaultRetryPolicy
import io.trama.saga.DefaultSagaExecutor
import io.trama.saga.MustacheTemplateRenderer
import io.trama.saga.RedisSagaEnqueuer
import io.trama.saga.SagaEnqueuer
import io.trama.saga.SagaExecution
import io.trama.saga.SagaExecutionProcessor
import io.trama.saga.SagaExecutionQueue
import io.trama.saga.SagaHttpClient
import io.trama.saga.SagaRepositoryStore
import io.trama.saga.redis.RedisClientProvider
import io.trama.saga.redis.RedisSagaExecutionStore
import io.trama.saga.redis.RedisSagaRateLimiter
import io.trama.saga.redis.SagaExecutionRedisConsumer
import io.trama.saga.redis.SagaExecutionRedisWriter
import io.trama.saga.store.DatabaseClient
import io.trama.saga.store.SagaRepository
import io.trama.telemetry.Metrics
import io.trama.telemetry.Tracing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

class RuntimeBootstrap(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry,
) {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private lateinit var database: DatabaseClient
    private lateinit var redis: RedisClientProvider
    private lateinit var httpClient: SagaHttpClient
    private var repository: SagaRepository? = null
    private var enqueuer: SagaEnqueuer? = null

    fun start() {
        database = DatabaseClient(config.database)
        redis = RedisClientProvider(config.redis)
        httpClient = SagaHttpClient(config.http)
        val repo = SagaRepository(database)
        repository = repo
        val store = when (config.runtime.store) {
            RuntimeStore.REDIS -> RedisSagaExecutionStore(redis, repo)
            RuntimeStore.POSTGRES -> SagaRepositoryStore(repo)
        }
        val enq = RedisSagaEnqueuer(redis, config.redis.queue.readyKey)
        enqueuer = enq
        val renderer = MustacheTemplateRenderer()
        val retryPolicy = DefaultRetryPolicy()
        val rateLimiter = RedisSagaRateLimiter(redis, config.rateLimit)
        val metricsRegistry = if (config.metrics.enabled) meterRegistry else SimpleMeterRegistry()
        val metrics = Metrics(metricsRegistry)

        val queue = SagaExecutionQueue(capacity = config.runtime.bufferSize)
        val writer = SagaExecutionRedisWriter(
            queue = queue,
            redis = redis,
            redisKey = config.redis.queue.readyKey,
            metrics = metrics,
        )
        val consumer = SagaExecutionRedisConsumer(
            redis = redis,
            readyKey = config.redis.queue.readyKey,
            inFlightKey = config.redis.queue.inFlightKey,
            batchSize = config.redis.consumer.batchSize,
            processingTimeoutMillis = config.redis.consumer.processingTimeoutMillis,
            metrics = metrics,
        )
        val executor = DefaultSagaExecutor(
            store = store,
            renderer = renderer,
            retryPolicy = retryPolicy,
            enqueuer = enq,
            httpClient = httpClient,
        )
        val processor = SagaExecutionProcessor(
            consumer = consumer,
            executor = executor,
            enqueuer = enq,
            rateLimiter = rateLimiter,
            metrics = metrics,
            bufferSize = config.runtime.bufferSize,
            emptyPollDelayMillis = config.runtime.emptyPollDelayMillis,
        )

        Tracing.initialize(config.telemetry)
        val maintenance = PartitionMaintenance(database, config.maintenance)

        scope.launch { writer.run() }
        scope.launch {
            consumer.runExpiredRequeuePoller(
                intervalMillis = config.redis.consumer.requeueIntervalMillis,
            )
        }
        scope.launch { processor.runProducer() }
        repeat(config.runtime.workerCount) { scope.launch { processor.runWorker() } }
        scope.launch { maintenance.runLoop() }
    }

    fun repositoryOrNull(): SagaRepository? {
        return repository
    }

    suspend fun enqueueRetry(execution: SagaExecution) {
        val enq = enqueuer ?: error("Runtime not initialized")
        enq.enqueue(execution, 0)
    }

    fun stop() {
        scope.cancel()
        redis.close()
        httpClient.close()
        database.close()
        Tracing.shutdown()
    }
}
