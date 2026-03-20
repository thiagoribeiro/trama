package run.trama.runtime

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.math.min
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import run.trama.config.AppConfig
import run.trama.config.RuntimeStore
import run.trama.saga.DefaultRetryPolicy
import run.trama.saga.DefaultSagaExecutor
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionProcessor
import run.trama.saga.SagaHttpClient
import run.trama.saga.SagaRepositoryStore
import run.trama.saga.redis.PodMembershipRegistry
import run.trama.saga.redis.ReadinessStatus
import run.trama.saga.redis.RedisShardKeyspace
import run.trama.saga.redis.RedisClientProvider
import run.trama.saga.redis.RendezvousShardAllocator
import run.trama.saga.redis.RedisSagaExecutionStore
import run.trama.saga.redis.RedisSagaRateLimiter
import run.trama.saga.redis.SagaExecutionRedisConsumer
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.SagaRepository
import run.trama.telemetry.Metrics
import run.trama.telemetry.Tracing

class RuntimeBootstrap(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry,
) {
    private val runtimeJob = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.Default + runtimeJob)
    private val stopped = AtomicBoolean(false)
    private lateinit var database: DatabaseClient
    private lateinit var redis: RedisClientProvider
    private lateinit var httpClient: SagaHttpClient
    private var metrics: Metrics? = null
    private var membership: PodMembershipRegistry? = null
    private var processor: SagaExecutionProcessor? = null
    private var repository: SagaRepository? = null
    private var enqueuer: SagaEnqueuer? = null
    private var heartbeatJob: Job? = null
    private var refreshJob: Job? = null
    private var requeueJob: Job? = null
    private var producerJob: Job? = null
    private val workerJobs = mutableListOf<Job>()
    private var maintenanceJob: Job? = null

    fun start() {
        val metricsRegistry = if (config.metrics.enabled) meterRegistry else SimpleMeterRegistry()
        database = DatabaseClient(config.database, metricsRegistry)
        redis = RedisClientProvider(config.redis)
        httpClient = SagaHttpClient(config.http)
        val repo = SagaRepository(database, config.database.pool.definitionCacheMaxSize)
        repository = repo
        val runtimeMetrics = Metrics(metricsRegistry)
        metrics = runtimeMetrics
        val keyspace = RedisShardKeyspace(
            queueKeyPrefix = config.redis.queue.keyPrefix,
            virtualShardCount = config.redis.sharding.virtualShardCount,
        )
        val store = when (config.runtime.store) {
            RuntimeStore.REDIS -> RedisSagaExecutionStore(redis, repo, keyspace)
            RuntimeStore.POSTGRES -> SagaRepositoryStore(repo)
        }
        val enq = RedisSagaEnqueuer(redis, keyspace)
        enqueuer = enq
        val renderer = MustacheTemplateRenderer()
        val retryPolicy = DefaultRetryPolicy()
        val rateLimiter = RedisSagaRateLimiter(redis, config.rateLimit, keyspace)
        val allocator = RendezvousShardAllocator(
            localPodId = config.redis.sharding.podId,
            virtualShardCount = config.redis.sharding.virtualShardCount,
        )
        val membershipRegistry = PodMembershipRegistry(
            redis = redis,
            membershipKey = config.redis.sharding.membershipKey,
            podId = config.redis.sharding.podId,
            membershipTtlMillis = config.redis.sharding.membershipTtlMillis,
            heartbeatIntervalMillis = config.redis.sharding.heartbeatIntervalMillis,
            refreshIntervalMillis = config.redis.sharding.refreshIntervalMillis,
            allocator = allocator,
            metrics = runtimeMetrics,
        )
        membership = membershipRegistry
        runBlocking { membershipRegistry.initialize() }
        val claimerCount = (config.redis.sharding.claimerCount ?: min(config.runtime.workerCount, 4)).coerceAtLeast(1)
        val consumer = SagaExecutionRedisConsumer(
            redis = redis,
            keyspace = keyspace,
            allocator = allocator,
            batchSize = config.redis.consumer.batchSize,
            processingTimeoutMillis = config.redis.consumer.processingTimeoutMillis,
            claimerCount = claimerCount,
            metrics = runtimeMetrics,
        )
        val executor = DefaultSagaExecutor(
            store = store,
            renderer = renderer,
            retryPolicy = retryPolicy,
            enqueuer = enq,
            httpClient = httpClient,
            metrics = runtimeMetrics,
            maxStepsPerExecution = config.runtime.maxStepsPerExecution,
        )
        val processor = SagaExecutionProcessor(
            consumer = consumer,
            executor = executor,
            enqueuer = enq,
            rateLimiter = rateLimiter,
            metrics = runtimeMetrics,
            bufferSize = config.runtime.bufferSize,
            emptyPollDelayMillis = config.runtime.emptyPollDelayMillis,
        )
        this.processor = processor

        Tracing.initialize(config.telemetry)
        val maintenance = PartitionMaintenance(database, config.maintenance)

        heartbeatJob = scope.launch { membershipRegistry.runHeartbeatLoop() }
        refreshJob = scope.launch { membershipRegistry.runRefreshLoop() }
        requeueJob = scope.launch {
            consumer.runExpiredRequeuePoller(
                intervalMillis = config.redis.consumer.requeueIntervalMillis,
            )
        }
        producerJob = scope.launch { processor.runProducer() }
        repeat(config.runtime.workerCount) { workerJobs += scope.launch { processor.runWorker() } }
        maintenanceJob = scope.launch { maintenance.runLoop() }
    }

    fun repositoryOrNull(): SagaRepository? {
        return repository
    }

    suspend fun enqueueRetry(execution: SagaExecution) {
        val enq = enqueuer ?: error("Runtime not initialized")
        enq.enqueue(execution, 0)
    }

    suspend fun readiness(): ReadinessStatus {
        val runtimeMetrics = metrics
        if (stopped.get()) {
            return ReadinessStatus(false, "shutting_down")
        }
        if (!config.runtime.enabled) {
            runtimeMetrics?.setRedisMembershipRefreshAgeMillis(0L)
            return ReadinessStatus(true, "ready")
        }
        return try {
            redis.withCommands { commands -> commands.ping() }
            membership?.readiness() ?: ReadinessStatus(false, "membership_unavailable")
        } catch (_: Exception) {
            ReadinessStatus(false, "redis_unreachable")
        }
    }

    fun stop() {
        if (!stopped.compareAndSet(false, true)) {
            return
        }

        runBlocking {
            processor?.stopPolling()
            runCatching { membership?.unregister() }

            listOfNotNull(heartbeatJob, refreshJob, requeueJob, maintenanceJob).forEach { it.cancel() }
            producerJob?.join()
            workerJobs.joinAll()
            runtimeJob.cancelAndJoin()
        }
        redis.close()
        httpClient.close()
        database.close()
        Tracing.shutdown()
    }
}
