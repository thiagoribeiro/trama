package run.trama.runtime

import run.trama.config.MaintenanceConfig
import run.trama.saga.store.DatabaseClient
import java.time.LocalDate
import java.time.ZoneOffset
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import net.logstash.logback.argument.StructuredArguments.kv

class PartitionMaintenance(
    private val db: DatabaseClient,
    private val config: MaintenanceConfig,
) {
    private val logger = LoggerFactory.getLogger(PartitionMaintenance::class.java)

    suspend fun runLoop() {
        while (true) {
            try {
                if (config.enabled) {
                    createPartitions()
                    dropOldPartitions()
                }
            } catch (ex: Exception) {
                logger.warn(
                    "partition maintenance failed",
                    kv("enabled", config.enabled),
                    ex
                )
            }
            delay(config.intervalMillis)
        }
    }

    fun createPartitions() {
        val startMonth = LocalDate.now(ZoneOffset.UTC)
            .withDayOfMonth(1)
            .minusMonths(config.partitionStartOffsetMonths.toLong())
        val endMonthExclusive = LocalDate.now(ZoneOffset.UTC)
            .withDayOfMonth(1)
            .plusMonths(config.partitionLookaheadMonths.toLong() + 1)

        db.withConnection { connection ->
            val stmt = connection.createStatement()
            var current = startMonth
            while (current.isBefore(endMonthExclusive)) {
                val partitionName = current.toString().substring(0, 7).replace("-", "")
                val from = current.atStartOfDay().toInstant(ZoneOffset.UTC)
                val to = current.plusMonths(1).atStartOfDay().toInstant(ZoneOffset.UTC)

                stmt.executeUpdate(
                    """
                    create table if not exists saga_execution_$partitionName
                    partition of saga_execution for values from ('$from') to ('$to')
                    """.trimIndent()
                )
                stmt.executeUpdate(
                    """
                    create table if not exists saga_step_result_$partitionName
                    partition of saga_step_result for values from ('$from') to ('$to')
                    """.trimIndent()
                )
                current = current.plusMonths(1)
            }
        }
    }

    fun dropOldPartitions() {
        val cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(config.retentionDays.toLong())
        val oldestMonthToKeep = cutoff.withDayOfMonth(1)

        db.withConnection { connection ->
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery(
                """
                select inhrelid::regclass::text as child
                from pg_inherits
                where inhparent in ('saga_execution'::regclass, 'saga_step_result'::regclass)
                """.trimIndent()
            )
            val toDrop = mutableListOf<String>()
            while (rs.next()) {
                val name = rs.getString("child")
                val suffix = name.substringAfterLast('_', "")
                if (suffix.length == 6) {
                    val month = LocalDate.parse(
                        suffix.substring(0, 4) + "-" + suffix.substring(4, 6) + "-01"
                    )
                    if (month.isBefore(oldestMonthToKeep)) {
                        toDrop.add(name)
                    }
                }
            }
            toDrop.forEach { table ->
                stmt.executeUpdate("drop table if exists $table")
            }
        }
    }
}
