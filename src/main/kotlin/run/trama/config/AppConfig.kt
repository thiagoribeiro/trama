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
)

data class RedisConfig(
    val topology: RedisTopology = RedisTopology.STANDALONE,
    val url: String = "redis://localhost:6379",
    val cluster: RedisClusterConfig = RedisClusterConfig(),
    val pool: RedisPoolConfig,
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
    val processingTimeoutMillis: Long = 60_000,
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
)

data class RuntimeConfig(
    val enabled: Boolean = true,
    val workerCount: Int = 4,
    val bufferSize: Int = 200,
    val emptyPollDelayMillis: Long = 50,
    val maxStepsPerExecution: Int = 25,
    val store: RuntimeStore = RuntimeStore.REDIS,
)

enum class RuntimeStore {
    REDIS,
    POSTGRES,
}

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
