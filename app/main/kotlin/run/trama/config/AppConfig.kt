package run.trama.config

data class AppConfig(
    val redis: RedisConfig,
    val database: DatabaseConfig,
    val runtime: RuntimeConfig,
    val http: HttpConfig,
    val telemetry: TelemetryConfig,
    val maintenance: MaintenanceConfig,
    val rateLimit: RateLimitConfig,
    val metrics: MetricsConfig,
    val callbackTimeoutScanner: CallbackTimeoutScannerConfig = CallbackTimeoutScannerConfig(),
    val joinCompletionScanner: JoinCompletionScannerConfig = JoinCompletionScannerConfig(),
    val sleep: SleepConfig = SleepConfig(),
    val reconciler: ReconcilerConfig = ReconcilerConfig(),
)

data class RedisConfig(
    val topology: RedisTopology = RedisTopology.STANDALONE,
    val url: String = "redis://localhost:6379",
    val cluster: RedisClusterConfig = RedisClusterConfig(),
    /**
     * Upper bound for any single Redis command. Commands are also rejected immediately while the
     * connection is down (instead of queueing until it returns), so callers see the outage at once.
     */
    val commandTimeoutMillis: Long = 5_000,
    /** Obsolete and ignored: Trama now uses one multiplexed connection (see RedisClientProvider). */
    val pool: RedisPoolConfig = RedisPoolConfig(),
    val queue: RedisQueueConfig,
    val consumer: RedisConsumerConfig,
    val sharding: RedisShardingConfig = RedisShardingConfig(),
)

enum class RedisTopology {
    STANDALONE,
    CLUSTER,
}

data class RedisClusterConfig(
    val nodes: List<String> = emptyList(),
)

data class RedisPoolConfig(
    val maxTotal: Int = 16,
    val maxIdle: Int = 16,
    val minIdle: Int = 0,
    val testOnBorrow: Boolean = true,
    val testWhileIdle: Boolean = true,
)

data class RedisQueueConfig(
    val keyPrefix: String = "saga:executions",
)

data class RedisConsumerConfig(
    val batchSize: Int = 50,
    /**
     * In-flight claim lease. Live claims are renewed every third of it (claim heartbeat), so it
     * only bounds how long work claimed by a dead pod waits before being re-delivered.
     */
    val processingTimeoutMillis: Long = 20_000,
    /** Obsolete and ignored: expired claims are now recovered by the claim script itself. */
    val requeueIntervalMillis: Long = 5_000,
)

data class RedisShardingConfig(
    val virtualShardCount: Int = 1024,
    val podId: String = System.getenv("HOSTNAME") ?: "unknown-pod",
    val membershipKey: String = "saga:runtime:pods",
    val membershipTtlMillis: Long = 10_000,
    val heartbeatIntervalMillis: Long = 3_000,
    val refreshIntervalMillis: Long = 2_000,
    val claimerCount: Int? = null,
)

data class DatabaseConfig(
    val host: String,
    val port: Int,
    val database: String,
    val user: String,
    val password: String,
    val pool: DatabasePoolConfig,
    val circuitBreaker: DatabaseCircuitBreakerConfig = DatabaseCircuitBreakerConfig(),
)

data class DatabaseCircuitBreakerConfig(
    val failureThreshold: Int = 5,
    val cooldownMillis: Long = 30_000,
)

data class DatabasePoolConfig(
    val maxPoolSize: Int = 10,
    val minIdle: Int = 1,
    val definitionCacheMaxSize: Int = 1000,
    /** Max staleness of the per-pod definition cache (see SagaRepository). */
    val definitionCacheTtlMillis: Long = 5_000,
)

data class RuntimeConfig(
    /**
     * Runs workers (claiming and executing work) in this process. With false the process only
     * serves the API: it still accepts runs and callbacks and enqueues them for worker processes.
     */
    val enabled: Boolean = true,
    val workerCount: Int = 4,
    /**
     * Executions claimed ahead of a free worker. A process holds at most workerCount + prefetch
     * claimed executions; null means workerCount.
     */
    val prefetch: Int? = null,
    /** Obsolete and ignored: claims are bounded by workerCount + prefetch instead. */
    val bufferSize: Int = 200,
    val emptyPollDelayMillis: Long = 50,
    /**
     * How often each claimer visits every owned shard instead of only those the due index points
     * to. A safety net for index entries lost to a crash or a Redis data loss.
     */
    val fullSweepIntervalMillis: Long = 5_000,
    /**
     * /healthz fails once the consumer has made no progress for this long (Redis unreachable or a
     * stuck process), so an orchestrator restarts the process. /readyz reacts much sooner.
     */
    val livenessStallMillis: Long = 120_000,
    val maxStepsPerExecution: Int = 25,
    val store: RuntimeStore = RuntimeStore.REDIS,
    val callback: CallbackConfig = CallbackConfig(),
)

data class CallbackConfig(
    /** Public base URL used to build callback URLs injected into async HTTP calls. */
    val baseUrl: String = "",
    /** HMAC-SHA256 secret for signing and validating callback tokens. */
    val hmacSecret: String = "",
    val hmacKid: String = "default",
)

enum class RuntimeStore {
    REDIS,
    POSTGRES,
}

/**
 * Per-workflow breaker: after [maxFailures] *worker failures* (exceptions while processing, i.e.
 * the runtime or its dependencies misbehaving) within [windowMillis], executions of that workflow
 * are delayed for [blockMillis]. Executions that end FAILED by their own logic do not count.
 */
data class RateLimitConfig(
    val enabled: Boolean = true,
    val maxFailures: Long = 5,
    val windowMillis: Long = 60_000,
    val blockMillis: Long = 60_000,
    val keyPrefix: String = "saga:rate",
)

data class MetricsConfig(
    val enabled: Boolean = true,
)

data class MaintenanceConfig(
    val enabled: Boolean = true,
    val partitionLookaheadMonths: Int = 13,
    val partitionStartOffsetMonths: Int = 1,
    val retentionDays: Int = 15,
    val intervalMillis: Long = 3_600_000,
)

data class SleepConfig(
    val maxChunkMillis: Long = 12 * 3_600_000L,
    val jitterMillis: Long = 60_000L,
)

data class CallbackTimeoutScannerConfig(
    val enabled: Boolean = true,
    /** How often the scanner runs, in milliseconds. */
    val intervalMillis: Long = 300_000,
    /** Grace period in seconds: skip executions whose deadline passed fewer than this many seconds ago. */
    val bufferSeconds: Long = 120,
    /** Maximum executions processed per scanner run. */
    val batchSize: Int = 100,
)

/**
 * Own config for [run.trama.runtime.JoinCompletionScanner] — deliberately a separate type/field
 * from [CallbackTimeoutScannerConfig] (even though the shape is the same) so disabling the
 * callback-timeout scanner can never silently disable the unrelated join-barrier backstop too.
 */
data class JoinCompletionScannerConfig(
    val enabled: Boolean = true,
    /** How often the scanner runs, in milliseconds. */
    val intervalMillis: Long = 30_000,
    /** Maximum barriers processed per scanner run. */
    val batchSize: Int = 100,
)

/**
 * [run.trama.runtime.ExecutionReconciler]: re-sends running or sleeping executions that should
 * have moved more than [staleAfterMillis] ago but did not (their queue item was lost, e.g. to a
 * Redis data loss). Must stay well above the longest single node (the HTTP request timeout).
 */
data class ReconcilerConfig(
    val enabled: Boolean = true,
    val intervalMillis: Long = 30_000,
    val staleAfterMillis: Long = 120_000,
    val batchSize: Int = 200,
)

data class HttpConfig(
    val connectTimeoutMillis: Long = 10_000,
    val requestTimeoutMillis: Long = 30_000,
    val socketTimeoutMillis: Long = 30_000,
)

data class TelemetryConfig(
    val enabled: Boolean = false,
    val serviceName: String = "trama",
    val otlpEndpoint: String = "http://localhost:4317",
)
