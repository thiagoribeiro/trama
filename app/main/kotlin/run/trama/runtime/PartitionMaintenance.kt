package run.trama.runtime

import run.trama.config.MaintenanceConfig
import run.trama.saga.store.DatabaseClient
import java.sql.Connection
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
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

    /**
     * Ensures one monthly partition per table from [MaintenanceConfig.partitionStartOffsetMonths]
     * back to [MaintenanceConfig.partitionLookaheadMonths] ahead.
     *
     * Bounds are UTC month starts, except that a new partition is anchored to the bounds of an
     * existing neighbouring month. The Liquibase bootstrap (schema_partitions.sql) casts month
     * dates to timestamptz in the session zone, which follows the JVM zone; on a non-UTC JVM its
     * partitions are shifted by the zone offset, and a plain UTC bound would overlap them.
     * Anchoring keeps the chain gap- and overlap-free for any existing layout (and is identical
     * to plain UTC bounds on UTC deployments).
     */
    suspend fun createPartitions() {
        val thisMonth = LocalDate.now(ZoneOffset.UTC).withDayOfMonth(1)
        val startMonth = thisMonth.minusMonths(config.partitionStartOffsetMonths.toLong())
        val endMonthExclusive = thisMonth.plusMonths(config.partitionLookaheadMonths.toLong() + 1)

        db.withConnection { connection ->
            val bounds = existingPartitionBounds(connection)
            connection.createStatement().use { stmt ->
                for (table in PARTITIONED_TABLES) {
                    var month = startMonth
                    while (month.isBefore(endMonthExclusive)) {
                        val name = partitionName(table, month)
                        if (name !in bounds) {
                            val from = bounds[partitionName(table, month.minusMonths(1))]?.to ?: month.utcStart()
                            val to = bounds[partitionName(table, month.plusMonths(1))]?.from ?: month.plusMonths(1).utcStart()
                            stmt.executeUpdate(
                                "create table if not exists $name partition of $table for values from ('$from') to ('$to')"
                            )
                            bounds[name] = Bounds(from, to)
                        }
                        month = month.plusMonths(1)
                    }
                }
            }
        }
    }

    suspend fun dropOldPartitions() {
        val cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(config.retentionDays.toLong())
        val oldestMonthToKeep = cutoff.withDayOfMonth(1)

        db.withConnection { connection ->
            val toDrop = mutableListOf<String>()
            connection.createStatement().use { stmt ->
                stmt.executeQuery(
                    """
                    select inhrelid::regclass::text as child
                    from pg_inherits
                    where inhparent in ($PARENTS_SQL)
                    """.trimIndent()
                ).use { rs ->
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
                }
                toDrop.forEach { table ->
                    stmt.executeUpdate("drop table if exists $table")
                }
            }
        }
    }

    private data class Bounds(val from: Instant, val to: Instant)

    /**
     * Range bounds of every existing child partition, keyed by name. pg_get_expr renders the
     * bounds as literals in the session zone and the cast parses them back in the same session,
     * so the round trip is exact whatever that zone is. One query per maintenance run.
     */
    private fun existingPartitionBounds(connection: Connection): MutableMap<String, Bounds> {
        val sql = """
            select c.relname,
                   (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \(''([^'']+)''\)'))[1]::timestamptz as lo,
                   (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \(''([^'']+)''\)'))[1]::timestamptz as hi
            from pg_inherits i
            join pg_class c on c.oid = i.inhrelid
            where i.inhparent in ($PARENTS_SQL)
        """.trimIndent()
        val bounds = mutableMapOf<String, Bounds>()
        connection.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                while (rs.next()) {
                    val lo = rs.getObject("lo", OffsetDateTime::class.java) ?: continue // e.g. a DEFAULT partition
                    val hi = rs.getObject("hi", OffsetDateTime::class.java) ?: continue
                    bounds[rs.getString("relname")] = Bounds(lo.toInstant(), hi.toInstant())
                }
            }
        }
        return bounds
    }

    private fun partitionName(table: String, month: LocalDate) =
        table + "_" + month.toString().substring(0, 7).replace("-", "")

    private fun LocalDate.utcStart(): Instant = atStartOfDay().toInstant(ZoneOffset.UTC)

    private companion object {
        /** Every time-partitioned table (see db.changelog-master.xml / schema_partitions.sql). */
        val PARTITIONED_TABLES = listOf("saga_execution", "saga_step_result", "saga_step_call")
        val PARENTS_SQL = PARTITIONED_TABLES.joinToString { "'$it'::regclass" }
    }
}
