package run.trama.saga.store

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import run.trama.config.DatabaseConfig
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class DatabaseClient(
    private val config: DatabaseConfig,
    meterRegistry: MeterRegistry,
) : AutoCloseable {
    private val dataSource: HikariDataSource
    private val circuitState = AtomicReference(CircuitState.CLOSED)
    private val consecutiveFailures = AtomicInteger(0)
    private val openedAt = AtomicLong(0L)

    private enum class CircuitState { CLOSED, OPEN, HALF_OPEN }

    init {
        val jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
        val dsConfig = HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            username = config.user
            password = config.password
            maximumPoolSize = config.pool.maxPoolSize
            minimumIdle = config.pool.minIdle
            isAutoCommit = true
            poolName = "saga-db-pool"
            metricsTrackerFactory = MicrometerMetricsTrackerFactory(meterRegistry)
        }
        dataSource = HikariDataSource(dsConfig)
        runMigrations()
    }

    private fun runMigrations() {
        dataSource.connection.use { conn ->
            val database = DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(JdbcConnection(conn))
            Liquibase("db/changelog/db.changelog-master.xml", ClassLoaderResourceAccessor(), database)
                .update("")
        }
    }

    suspend fun <T> withConnection(block: (Connection) -> T): T =
        withContext(Dispatchers.IO) {
            checkCircuit()
            try {
                val result = dataSource.connection.use { connection -> block(connection) }
                onSuccess()
                result
            } catch (ex: Exception) {
                onFailure()
                throw ex
            }
        }

    private fun checkCircuit() {
        when (circuitState.get()) {
            CircuitState.CLOSED -> return
            CircuitState.OPEN -> {
                val cooldown = config.circuitBreaker.cooldownMillis
                if (System.currentTimeMillis() - openedAt.get() >= cooldown) {
                    circuitState.compareAndSet(CircuitState.OPEN, CircuitState.HALF_OPEN)
                } else {
                    throw DatabaseCircuitOpenException("Postgres circuit breaker is open — backing off")
                }
            }
            CircuitState.HALF_OPEN -> return
            null -> return
        }
    }

    private fun onSuccess() {
        consecutiveFailures.set(0)
        circuitState.set(CircuitState.CLOSED)
    }

    private fun onFailure() {
        val failures = consecutiveFailures.incrementAndGet()
        if (failures >= config.circuitBreaker.failureThreshold) {
            if (circuitState.compareAndSet(CircuitState.CLOSED, CircuitState.OPEN) ||
                circuitState.compareAndSet(CircuitState.HALF_OPEN, CircuitState.OPEN)
            ) {
                openedAt.set(System.currentTimeMillis())
            }
        }
    }

    override fun close() {
        dataSource.close()
    }
}

class DatabaseCircuitOpenException(message: String) : RuntimeException(message)
