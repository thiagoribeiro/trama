package run.trama.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.ExperimentalHoplite
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.addEnvironmentSource

object ConfigLoader {
    @OptIn(ExperimentalHoplite::class)
    fun load(): AppConfig {
        val base = ConfigLoaderBuilder.default()
            .withExplicitSealedTypes()
            .addResourceSource("/application.yaml", optional = true)
            .addEnvironmentSource()
            .build()
            .loadConfigOrThrow<AppConfig>()
        val runtimeEnabled = System.getProperty("runtime.enabled")
            ?: System.getenv("RUNTIME_ENABLED")
        val metricsEnabled = System.getProperty("metrics.enabled")
            ?: System.getenv("METRICS_ENABLED")
        val telemetryEnabled = System.getProperty("telemetry.enabled")
            ?: System.getenv("TELEMETRY_ENABLED")
        val redisUrl = System.getProperty("redis.url")
            ?: System.getenv("REDIS_URL")
        val dbHost = System.getProperty("database.host")
            ?: System.getenv("DATABASE_HOST")
        val dbPort = System.getProperty("database.port")
            ?: System.getenv("DATABASE_PORT")
        val dbName = System.getProperty("database.database")
            ?: System.getenv("DATABASE_DATABASE")
        val dbUser = System.getProperty("database.user")
            ?: System.getenv("DATABASE_USER")
        val dbPassword = System.getProperty("database.password")
            ?: System.getenv("DATABASE_PASSWORD")
        val callbackBaseUrl = System.getProperty("runtime.callback.baseUrl")
            ?: System.getenv("RUNTIME_CALLBACK_BASEURL")
        val callbackHmacSecret = System.getProperty("runtime.callback.hmacSecret")
            ?: System.getenv("RUNTIME_CALLBACK_HMACSECRET")
        val callbackHmacKid = System.getProperty("runtime.callback.hmacKid")
            ?: System.getenv("RUNTIME_CALLBACK_HMACKID")
        val emptyPollDelayMillis = System.getProperty("runtime.emptyPollDelayMillis")
            ?: System.getenv("RUNTIME_EMPTYPOLLDELAYMILLIS")
        val virtualShardCount = System.getProperty("redis.sharding.virtualShardCount")
            ?: System.getenv("REDIS_SHARDING_VIRTUALSHARDCOUNT")
        val definitionCacheTtlMillis = System.getProperty("database.pool.definitionCacheTtlMillis")
            ?: System.getenv("DATABASE_POOL_DEFINITIONCACHETTLMILLIS")
        val redisTopology = System.getProperty("redis.topology")
            ?: System.getenv("REDIS_TOPOLOGY")
        val redisClusterNodes = System.getProperty("redis.cluster.nodes")
            ?: System.getenv("REDIS_CLUSTER_NODES")

        val redisCommandTimeoutMillis = System.getProperty("redis.commandTimeoutMillis")
            ?: System.getenv("REDIS_COMMANDTIMEOUTMILLIS")
        val runtimePrefetch = System.getProperty("runtime.prefetch")
            ?: System.getenv("RUNTIME_PREFETCH")
        val runtimeWorkerCount = System.getProperty("runtime.workerCount")
            ?: System.getenv("RUNTIME_WORKERCOUNT")
        val reconcilerEnabled = System.getProperty("reconciler.enabled")
            ?: System.getenv("RECONCILER_ENABLED")
        val reconcilerStaleAfterMillis = System.getProperty("reconciler.staleAfterMillis")
            ?: System.getenv("RECONCILER_STALEAFTERMILLIS")
        val reconcilerIntervalMillis = System.getProperty("reconciler.intervalMillis")
            ?: System.getenv("RECONCILER_INTERVALMILLIS")
        val rateLimitEnabled = System.getProperty("rateLimit.enabled")
            ?: System.getenv("RATELIMIT_ENABLED")

        var config = base
        runtimeEnabled?.toBooleanStrictOrNull()?.let {
            config = config.copy(runtime = config.runtime.copy(enabled = it))
        }
        metricsEnabled?.toBooleanStrictOrNull()?.let {
            config = config.copy(metrics = config.metrics.copy(enabled = it))
        }
        telemetryEnabled?.toBooleanStrictOrNull()?.let {
            config = config.copy(telemetry = config.telemetry.copy(enabled = it))
        }
        if (!redisUrl.isNullOrBlank()) {
            config = config.copy(redis = config.redis.copy(url = redisUrl))
        }
        if (!dbHost.isNullOrBlank() || !dbPort.isNullOrBlank() || !dbName.isNullOrBlank()
            || !dbUser.isNullOrBlank() || !dbPassword.isNullOrBlank()
        ) {
            config = config.copy(
                database = config.database.copy(
                    host = dbHost ?: config.database.host,
                    port = dbPort?.toIntOrNull() ?: config.database.port,
                    database = dbName ?: config.database.database,
                    user = dbUser ?: config.database.user,
                    password = dbPassword ?: config.database.password,
                )
            )
        }
        if (!callbackBaseUrl.isNullOrBlank() || !callbackHmacSecret.isNullOrBlank() || !callbackHmacKid.isNullOrBlank()) {
            config = config.copy(
                runtime = config.runtime.copy(
                    callback = config.runtime.callback.copy(
                        baseUrl = callbackBaseUrl ?: config.runtime.callback.baseUrl,
                        hmacSecret = callbackHmacSecret ?: config.runtime.callback.hmacSecret,
                        hmacKid = callbackHmacKid ?: config.runtime.callback.hmacKid,
                    )
                )
            )
        }
        emptyPollDelayMillis?.toLongOrNull()?.let {
            config = config.copy(runtime = config.runtime.copy(emptyPollDelayMillis = it))
        }
        virtualShardCount?.toIntOrNull()?.let {
            config = config.copy(redis = config.redis.copy(sharding = config.redis.sharding.copy(virtualShardCount = it)))
        }
        redisTopology?.trim()?.uppercase()
            ?.let { name -> RedisTopology.entries.firstOrNull { it.name == name } }
            ?.let { config = config.copy(redis = config.redis.copy(topology = it)) }
        redisClusterNodes
            ?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() }
            ?.takeIf { it.isNotEmpty() }
            ?.let { config = config.copy(redis = config.redis.copy(cluster = config.redis.cluster.copy(nodes = it))) }
        definitionCacheTtlMillis?.toLongOrNull()?.let {
            config = config.copy(database = config.database.copy(pool = config.database.pool.copy(definitionCacheTtlMillis = it)))
        }
        redisCommandTimeoutMillis?.toLongOrNull()?.let {
            config = config.copy(redis = config.redis.copy(commandTimeoutMillis = it))
        }
        runtimePrefetch?.toIntOrNull()?.let {
            config = config.copy(runtime = config.runtime.copy(prefetch = it))
        }
        runtimeWorkerCount?.toIntOrNull()?.let {
            config = config.copy(runtime = config.runtime.copy(workerCount = it))
        }
        reconcilerEnabled?.toBooleanStrictOrNull()?.let {
            config = config.copy(reconciler = config.reconciler.copy(enabled = it))
        }
        reconcilerStaleAfterMillis?.toLongOrNull()?.let {
            config = config.copy(reconciler = config.reconciler.copy(staleAfterMillis = it))
        }
        reconcilerIntervalMillis?.toLongOrNull()?.let {
            config = config.copy(reconciler = config.reconciler.copy(intervalMillis = it))
        }
        rateLimitEnabled?.toBooleanStrictOrNull()?.let {
            config = config.copy(rateLimit = config.rateLimit.copy(enabled = it))
        }
        return config
    }
}
