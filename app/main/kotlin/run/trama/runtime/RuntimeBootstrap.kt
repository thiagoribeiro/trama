package run.trama.runtime

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.math.min
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import run.trama.config.AppConfig
import run.trama.config.RuntimeStore
import run.trama.saga.DefaultRetryPolicy
import run.trama.saga.MustacheTemplateRenderer
import run.trama.saga.RedisSagaEnqueuer
import run.trama.saga.SagaEnqueuer
import run.trama.saga.SagaRepositoryStore
import run.trama.saga.SagaExecution
import run.trama.saga.SagaExecutionProcessor
import run.trama.saga.SagaHttpClient
import run.trama.saga.callback.CallbackReceiver
import run.trama.saga.callback.CallbackTokenService
import run.trama.saga.callback.CallbackUrlFactory
import run.trama.saga.workflow.WorkflowExecutor
import run.trama.saga.redis.OrphanedShardMigrator
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

/**
 * Composition root. [start] always brings up the core every process needs to serve the API
 * (Postgres, Redis, store, enqueuer, callback receiver); with `runtime.enabled` it also starts the
 * workers: membership, claimers, processor and the background loops.
 */
class RuntimeBootstrap(
    private val config: AppConfig,
    private val meterRegistry: MeterRegistry,
) {
    private val logger = LoggerFactory.getLogger(RuntimeBootstrap::class.java)
    private val runtimeJob = SupervisorJob()
    private val scope = CoroutineScope(Dispatchers.Default + runtimeJob)
    private val stopped = AtomicBoolean(false)
    private var database: DatabaseClient? = null
    private var redis: RedisClientProvider? = null
    private var httpClient: SagaHttpClient? = null
    private var metrics: Metrics? = null
    private var membership: PodMembershipRegistry? = null
    private var consumer: SagaExecutionRedisConsumer? = null
    private var processor: SagaExecutionProcessor? = null
    private var repository: SagaRepository? = null
    private var enqueuer: SagaEnqueuer? = null
    private var store: run.trama.saga.SagaExecutionStore? = null
    private var callbackReceiver: CallbackReceiver? = null
    private var heartbeatJob: Job? = null
    private var refreshJob: Job? = null
    private var claimHeartbeatJob: Job? = null
    private var producerJob: Job? = null
    private val workerJobs = mutableListOf<Job>()
    private val loopJobs = mutableListOf<Job>()

    fun start() {
        val metricsRegistry = if (config.metrics.enabled) meterRegistry else SimpleMeterRegistry()
        val database = DatabaseClient(config.database, metricsRegistry).also { this.database = it }
        val redis = RedisClientProvider(config.redis).also { this.redis = it }
        val repo = SagaRepository(database, config.database.pool.definitionCacheMaxSize, config.database.pool.definitionCacheTtlMillis)
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
        this.store = store
        val enq = RedisSagaEnqueuer(redis, keyspace, runtimeMetrics)
        enqueuer = enq
        val retryPolicy = DefaultRetryPolicy()
        val callbackCfg = config.runtime.callback
        val callbackTokenService = if (callbackCfg.hmacSecret.isNotBlank()) {
            CallbackTokenService(callbackCfg.hmacSecret, callbackCfg.hmacKid)
        } else null
        if (callbackTokenService != null) {
            callbackReceiver = CallbackReceiver(
                store = store,
                enqueuer = enq,
                tokenService = callbackTokenService,
                retryPolicy = retryPolicy,
                metrics = runtimeMetrics,
            )
        }
        Tracing.initialize(config.telemetry)

        if (config.runtime.enabled) {
            startWorkers(redis, repo, store, enq, keyspace, retryPolicy, callbackTokenService, runtimeMetrics, database)
        }
    }

    private fun startWorkers(
        redis: RedisClientProvider,
        repo: SagaRepository,
        store: run.trama.saga.SagaExecutionStore,
        enq: SagaEnqueuer,
        keyspace: RedisShardKeyspace,
        retryPolicy: DefaultRetryPolicy,
        callbackTokenService: CallbackTokenService?,
        runtimeMetrics: Metrics,
        database: DatabaseClient,
    ) {
        val httpClient = SagaHttpClient(config.http).also { this.httpClient = it }
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
            fullSweepIntervalMillis = config.runtime.fullSweepIntervalMillis,
        )
        this.consumer = consumer
        val callbackCfg = config.runtime.callback
        val callbackUrlFactory = if (callbackCfg.baseUrl.isNotBlank()) {
            CallbackUrlFactory(callbackCfg.baseUrl)
        } else null

        val executor = WorkflowExecutor(
            store = store,
            renderer = MustacheTemplateRenderer(),
            retryPolicy = retryPolicy,
            enqueuer = enq,
            httpClient = httpClient,
            metrics = runtimeMetrics,
            maxNodesPerExecution = config.runtime.maxStepsPerExecution,
            sleepMaxChunkMillis = config.sleep.maxChunkMillis,
            sleepJitterMillis = config.sleep.jitterMillis,
            callbackTokenService = callbackTokenService,
            callbackUrlFactory = callbackUrlFactory,
        )
        val workerCount = config.runtime.workerCount.coerceAtLeast(1)
        val processor = SagaExecutionProcessor(
            consumer = consumer,
            executor = executor,
            enqueuer = enq,
            rateLimiter = rateLimiter,
            metrics = runtimeMetrics,
            claimCapacity = workerCount + (config.runtime.prefetch ?: workerCount).coerceAtLeast(0),
            emptyPollDelayMillis = config.runtime.emptyPollDelayMillis,
        )
        this.processor = processor

        val maintenance = PartitionMaintenance(database, config.maintenance)
        val callbackScanner = CallbackTimeoutScanner(
            repository = repo,
            enqueuer = enq,
            metrics = runtimeMetrics,
            config = config.callbackTimeoutScanner,
        )
        val joinScanner = JoinCompletionScanner(
            repository = repo,
            resumer = executor,
            config = config.joinCompletionScanner,
        )
        val reconciler = ExecutionReconciler(
            repository = repo,
            enqueuer = enq,
            metrics = runtimeMetrics,
            config = config.reconciler,
        )

        heartbeatJob = scope.launch { membershipRegistry.runHeartbeatLoop() }
        refreshJob = scope.launch { membershipRegistry.runRefreshLoop() }
        claimHeartbeatJob = scope.launch { consumer.runClaimHeartbeat() }
        producerJob = scope.launch { processor.runProducer() }
        repeat(workerCount) { workerJobs += scope.launch { processor.runWorker() } }
        loopJobs += scope.launch { maintenance.runLoop() }
        loopJobs += scope.launch { callbackScanner.runLoop() }
        loopJobs += scope.launch { joinScanner.runLoop() }
        loopJobs += scope.launch { reconciler.runLoop() }
        loopJobs += scope.launch { OrphanedShardMigrator(redis, keyspace, enq).runLoop() }
    }

    fun repositoryOrNull(): SagaRepository? = repository

    fun callbackReceiverOrNull(): CallbackReceiver? = callbackReceiver

    /**
     * Creates a new execution's row, then enqueues it. Once the row exists the run is accepted:
     * if Redis cannot take the queue item right now, the reconciler delivers it later instead of
     * the caller seeing an error for a run that would still execute.
     */
    suspend fun submit(execution: SagaExecution) {
        val s = store ?: error("Runtime not initialized")
        val enq = enqueuer ?: error("Runtime not initialized")
        s.admit(listOf(execution))
        try {
            enq.enqueue(execution, 0)
        } catch (ex: CancellationException) {
            throw ex
        } catch (ex: Exception) {
            metrics?.recordRedisError("enqueue")
            logger.warn("run accepted but not queued; the reconciler will deliver it", kv("sagaId", execution.id.toString()), ex)
        }
    }

    /**
     * Restarts a FAILED execution from [execution]'s state (built by the retry endpoint).
     * Returns false when it is not, or no longer, FAILED.
     */
    suspend fun retry(execution: SagaExecution): Boolean {
        val repo = repository ?: error("Runtime not initialized")
        val enq = enqueuer ?: error("Runtime not initialized")
        val prepared = repo.prepareRetry(execution) ?: return false
        enq.enqueue(prepared, 0)
        return true
    }

    fun storeOrNull(): run.trama.saga.SagaExecutionStore? = store

    suspend fun wakeExecution(executionId: java.util.UUID): WakeResult {
        val s = store ?: return WakeResult.RuntimeDisabled
        val repo = repository ?: return WakeResult.RuntimeDisabled
        val enq = enqueuer ?: return WakeResult.RuntimeDisabled
        val status = repo.getExecutionStatus(executionId) ?: return WakeResult.NotFound
        if (status.status != "SLEEPING") return WakeResult.NotSleeping
        val entry = s.peekSleeping(executionId) ?: return WakeResult.AlreadyWaking
        val sleeping = entry.execution.state as? run.trama.saga.ExecutionState.Sleeping ?: return WakeResult.NotSleeping
        // Re-deliver the sleep with wakeAt = now instead of rebuilding the next state here: the
        // executor's normal wake-up path then runs exactly once, whichever queue copy wins. The
        // new wake time is checkpointed first, which makes the original sleep's queue item stale.
        val now = java.time.Instant.now()
        val woken = entry.execution.copy(state = sleeping.copy(wakeAt = now), checkpointSeq = entry.execution.checkpointSeq + 1)
        try {
            s.checkpoint(woken, woken.checkpointSeq, now, parking = run.trama.saga.Parking.Sleep(now))
        } catch (_: run.trama.saga.StaleCheckpointException) {
            return WakeResult.AlreadyWaking
        }
        // Checkpointed above: if Redis cannot take the item now, the reconciler delivers it.
        runCatching { enq.enqueue(woken, 0) }
            .onFailure { logger.warn("wake applied but not queued; the reconciler will deliver it", kv("sagaId", executionId.toString()), it) }
        return WakeResult.Woken
    }

    enum class WakeResult { Woken, AlreadyWaking, NotSleeping, NotFound, RuntimeDisabled }

    /**
     * Liveness: false only when this process's consumer has made no progress for
     * `runtime.livenessStallMillis` — it is stuck and a restart is the remedy.
     */
    fun live(): Boolean {
        val c = consumer ?: return true
        return System.currentTimeMillis() - c.lastProgressAtMillis() < config.runtime.livenessStallMillis
    }

    suspend fun readiness(): ReadinessStatus {
        val runtimeMetrics = metrics
        if (stopped.get()) {
            return ReadinessStatus(false, "shutting_down")
        }
        val redis = redis ?: return ReadinessStatus(false, "not_started")
        val pinged = try {
            redis.withCommands { commands -> commands.ping() }
            true
        } catch (_: Exception) {
            false
        }
        if (!pinged) return ReadinessStatus(false, "redis_unreachable")
        if (!config.runtime.enabled) {
            runtimeMetrics?.setRedisMembershipRefreshAgeMillis(0L)
            return ReadinessStatus(true, "ready")
        }
        consumer?.let { c ->
            val stalledFor = System.currentTimeMillis() - c.lastProgressAtMillis()
            if (stalledFor > maxOf(5_000L, 3 * config.redis.consumer.processingTimeoutMillis)) {
                return ReadinessStatus(false, "consumer_stalled")
            }
        }
        return membership?.readiness() ?: ReadinessStatus(false, "membership_unavailable")
    }

    fun stop() {
        if (!stopped.compareAndSet(false, true)) {
            return
        }

        runBlocking {
            processor?.stopPolling()
            runCatching { membership?.unregister() }

            (listOfNotNull(heartbeatJob, refreshJob) + loopJobs).forEach { it.cancel() }
            producerJob?.join()
            workerJobs.joinAll()
            // Only after the drain: claims still being processed must keep being renewed.
            claimHeartbeatJob?.cancel()
            runtimeJob.cancelAndJoin()
        }
        redis?.close()
        httpClient?.close()
        database?.close()
        Tracing.shutdown()
    }
}
