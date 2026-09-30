package run.trama.runtime

import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import run.trama.config.MaintenanceConfig
import run.trama.saga.store.DatabaseClient
import run.trama.saga.store.IntegrationDb

class PartitionMaintenanceTest {

    @BeforeTest
    fun setUp() = IntegrationDb.assumeDocker()

    private fun suffix(month: LocalDate) = month.toString().substring(0, 7).replace("-", "")
    private val thisMonth: LocalDate get() = LocalDate.now(ZoneOffset.UTC).withDayOfMonth(1)

    private suspend fun tableExists(name: String, db: DatabaseClient = IntegrationDb.client): Boolean = db.withConnection { c ->
        c.prepareStatement("select to_regclass(?) is not null").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> rs.next(); rs.getBoolean(1) }
        }
    }

    private suspend fun createOldPartitions(month: LocalDate, tables: List<String>) = IntegrationDb.client.withConnection { c ->
        val from = month.atStartOfDay().toInstant(ZoneOffset.UTC)
        val to = month.plusMonths(1).atStartOfDay().toInstant(ZoneOffset.UTC)
        c.createStatement().use { st ->
            tables.forEach { t ->
                st.executeUpdate("create table if not exists ${t}_${suffix(month)} partition of $t for values from ('$from') to ('$to')")
            }
        }
    }

    @Test
    fun `createPartitions extends the window Liquibase created and is idempotent`() = runBlocking<Unit> {
        IntegrationDb.freshClient("UTC").use { db ->
            val maintenance = PartitionMaintenance(db, MaintenanceConfig(partitionLookaheadMonths = 20))
            maintenance.createPartitions()
            maintenance.createPartitions()

            val far = suffix(thisMonth.plusMonths(20))
            assertTrue(tableExists("saga_execution_$far", db))
            assertTrue(tableExists("saga_step_result_$far", db))
        }
    }

    @Disabled(
        "BUG: schema_partitions.sql (Liquibase) computes partition bounds with ::timestamptz in the JDBC " +
            "session zone, which pgjdbc takes from the JVM default zone, while PartitionMaintenance uses UTC. " +
            "With TZ=America/Sao_Paulo the next month's partition overlaps the previous one by 3h, so every " +
            "createPartitions run throws and no partition beyond the startup window is ever created.",
    )
    @Test
    fun `createPartitions works when the JVM time zone is not UTC`() = runBlocking<Unit> {
        IntegrationDb.freshClient("America/Sao_Paulo").use { db ->
            PartitionMaintenance(db, MaintenanceConfig(partitionLookaheadMonths = 14)).createPartitions()
            assertTrue(tableExists("saga_execution_${suffix(thisMonth.plusMonths(14))}", db))
        }
    }

    @Disabled(
        "BUG: PartitionMaintenance.createPartitions never creates saga_step_call partitions. Only the " +
            "runAlways Liquibase changeset at startup does, so a pod running past its initial 13-month window " +
            "starts failing every step-call insert.",
    )
    @Test
    fun `createPartitions also covers saga_step_call`() = runBlocking<Unit> {
        IntegrationDb.freshClient("UTC").use { db ->
            PartitionMaintenance(db, MaintenanceConfig(partitionLookaheadMonths = 22)).createPartitions()
            assertTrue(tableExists("saga_step_call_${suffix(thisMonth.plusMonths(22))}", db))
        }
    }

    @Test
    fun `dropOldPartitions drops months past retention and keeps the current one`() = runBlocking<Unit> {
        val old = LocalDate.of(2019, 3, 1)
        createOldPartitions(old, listOf("saga_execution", "saga_step_result"))

        PartitionMaintenance(IntegrationDb.client, MaintenanceConfig(retentionDays = 15)).dropOldPartitions()

        assertFalse(tableExists("saga_execution_${suffix(old)}"))
        assertFalse(tableExists("saga_step_result_${suffix(old)}"))
        assertTrue(tableExists("saga_execution_${suffix(thisMonth)}"))
        assertTrue(tableExists("saga_step_result_${suffix(thisMonth)}"))
    }

    @Disabled(
        "BUG: dropOldPartitions only inspects children of saga_execution and saga_step_result, so " +
            "saga_step_call (which stores full request/response bodies) is never pruned and grows forever.",
    )
    @Test
    fun `dropOldPartitions also prunes saga_step_call`() = runBlocking<Unit> {
        val old = LocalDate.of(2019, 4, 1)
        createOldPartitions(old, listOf("saga_step_call"))
        PartitionMaintenance(IntegrationDb.client, MaintenanceConfig(retentionDays = 15)).dropOldPartitions()
        assertFalse(tableExists("saga_step_call_${suffix(old)}"))
    }
}
