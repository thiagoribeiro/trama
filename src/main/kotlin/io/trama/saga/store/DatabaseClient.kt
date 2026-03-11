package run.trama.saga.store

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import run.trama.config.DatabaseConfig
import java.sql.Connection

class DatabaseClient(
    private val config: DatabaseConfig,
) : AutoCloseable {
    private val dataSource: HikariDataSource

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
        }
        dataSource = HikariDataSource(dsConfig)
    }

    fun <T> withConnection(block: (Connection) -> T): T {
        dataSource.connection.use { connection ->
            return block(connection)
        }
    }

    override fun close() {
        dataSource.close()
    }
}
