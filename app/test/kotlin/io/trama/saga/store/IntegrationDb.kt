package run.trama.saga.store

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.testcontainers.DockerClientFactory
import run.trama.config.DatabaseConfig
import run.trama.config.DatabasePoolConfig
import run.trama.e2e.E2EContainers

/**
 * Liquibase-migrated Postgres for integration tests, reusing the JVM-wide E2E container so
 * each test class doesn't pay for (and risk flaking on) its own container start. Tests must
 * isolate their data with unique names/ids since the database is shared.
 */
object IntegrationDb {
    val client: DatabaseClient by lazy {
        val pg = E2EContainers.postgres
        DatabaseClient(
            DatabaseConfig(
                host = pg.host,
                port = pg.firstMappedPort,
                database = pg.databaseName,
                user = pg.username,
                password = pg.password,
                pool = DatabasePoolConfig(),
            ),
            SimpleMeterRegistry(),
        )
    }

    /** Redis client against the JVM-wide E2E Redis container. Use unique key prefixes per test. */
    val redis: run.trama.saga.redis.RedisClientProvider by lazy {
        val r = E2EContainers.redis
        run.trama.saga.redis.RedisClientProvider(
            run.trama.config.RedisConfig(
                url = "redis://${r.host}:${r.getMappedPort(6379)}",
                pool = run.trama.config.RedisPoolConfig(),
                queue = run.trama.config.RedisQueueConfig(),
                consumer = run.trama.config.RedisConsumerConfig(),
            ),
        )
    }

    /**
     * Creates a brand-new database in the shared container and migrates it while the JVM default
     * time zone is [timeZone]. pgjdbc sends the JVM zone as the session TimeZone, so this controls
     * how Liquibase's SQL (e.g. `::timestamptz` casts) interprets local dates.
     */
    fun freshClient(timeZone: String): DatabaseClient {
        val pg = E2EContainers.postgres
        val dbName = "it_" + java.util.UUID.randomUUID().toString().replace("-", "").take(12)
        java.sql.DriverManager.getConnection(pg.jdbcUrl, pg.username, pg.password).use { c ->
            c.createStatement().use { it.executeUpdate("create database $dbName") }
        }
        val previous = java.util.TimeZone.getDefault()
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(timeZone))
        try {
            return DatabaseClient(
                DatabaseConfig(
                    host = pg.host,
                    port = pg.firstMappedPort,
                    database = dbName,
                    user = pg.username,
                    password = pg.password,
                    pool = DatabasePoolConfig(maxPoolSize = 2, minIdle = 0),
                ),
                SimpleMeterRegistry(),
            )
        } finally {
            java.util.TimeZone.setDefault(previous)
        }
    }

    /** Marks the test as skipped (not silently passed) when no container runtime is available. */
    fun assumeDocker() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable, "Docker/Podman not available")
    }
}
