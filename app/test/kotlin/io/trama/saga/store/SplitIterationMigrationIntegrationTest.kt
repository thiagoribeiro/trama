package run.trama.saga.store

import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import run.trama.saga.ExecutionState
import run.trama.saga.FailureHandling
import run.trama.saga.NodeActionDef
import run.trama.saga.NodeDefinition
import run.trama.saga.Parking
import run.trama.saga.RecordingEnqueuer
import run.trama.saga.RecordingHttp
import run.trama.saga.SagaDefinitionV2
import run.trama.saga.SagaExecution
import run.trama.saga.SagaRepositoryStore
import run.trama.saga.TaskMode
import run.trama.saga.httpCall
import run.trama.saga.testExecutor
import run.trama.saga.v2Execution

/**
 * Changeset 010 adds the split iteration to the join barrier key. A parent that an older version
 * parked on a join (barrier rows, waiting_state and branch checkpoints without an iteration) must
 * still be resumed once the new version runs: by its last branch and by the stalled-join scanner.
 */
class SplitIterationMigrationIntegrationTest {

    /** Changesets 001 … 009 (11 changesets): the schema of the version before 010. */
    private val changesetsBefore010 = 11

    private val definition = SagaDefinitionV2(
        name = "split-iteration-migration-${UUID.randomUUID()}",
        version = "1",
        failureHandling = FailureHandling.Retry(0, 0),
        entrypoint = "fan",
        nodes = listOf(
            NodeDefinition.Split("fan", branches = listOf("a", "b"), join = "fan-in"),
            NodeDefinition.Task("a", NodeActionDef(TaskMode.SYNC, httpCall("http://a/"))),
            NodeDefinition.Task("b", NodeActionDef(TaskMode.SYNC, httpCall("http://b/"))),
            NodeDefinition.Join("fan-in"),
        ),
    )

    private fun java.sql.Connection.hasColumn(table: String, column: String): Boolean =
        prepareStatement("SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?").use { ps ->
            ps.setString(1, table)
            ps.setString(2, column)
            ps.executeQuery().use { it.next() }
        }

    @Test
    fun `a join parked before the migration still resumes after it`() = runBlocking<Unit> {
        IntegrationDb.assumeDocker()
        val dbName = IntegrationDb.createDatabase()
        IntegrationDb.migratePartially(dbName, changesetsBefore010)

        val parent = v2Execution(definition)
        // The old child id: no iteration in it.
        val childIds = listOf("a", "b").associateWith { UUID.nameUUIDFromBytes("${parent.id}:fan:$it".toByteArray()) }

        // The barrier exactly as the old version's registerJoinBarrier wrote it.
        IntegrationDb.connect(dbName).use { c ->
            assertFalse(c.hasColumn("saga_join_barrier", "split_iteration"), "staged schema must predate changeset 010")
            val started = java.sql.Timestamp.from(parent.startedAt)
            c.prepareStatement(
                """
                INSERT INTO saga_join_barrier (parent_id, parent_started_at, split_node_id, join_node_id, expected_count, arrived_count, created_at)
                VALUES (?, ?, 'fan', 'fan-in', 2, 0, now())
                """.trimIndent(),
            ).use { ps -> ps.setObject(1, parent.id); ps.setTimestamp(2, started); ps.executeUpdate() }
            childIds.forEach { (branch, childId) ->
                c.prepareStatement(
                    """
                    INSERT INTO saga_join_branch (parent_id, parent_started_at, split_node_id, branch_id, child_id, child_started_at, created_at)
                    VALUES (?, ?, 'fan', ?, ?, ?, now())
                    """.trimIndent(),
                ).use { ps ->
                    ps.setObject(1, parent.id); ps.setTimestamp(2, started); ps.setString(3, branch)
                    ps.setObject(4, childId); ps.setTimestamp(5, started); ps.executeUpdate()
                }
            }
        }

        IntegrationDb.clientFor(dbName).use { client ->
            val repo = SagaRepository(client)
            val store = SagaRepositoryStore(repo)

            // Branches and the parked parent as the old version left them: no iteration anywhere,
            // so they decode as LEGACY_SPLIT_ITERATION.
            val children = childIds.map { (branch, childId) ->
                parent.copy(
                    id = childId,
                    state = ExecutionState.InProgress(activeNodeId = branch),
                    parentExecutionId = parent.id,
                    parentStartedAt = parent.startedAt,
                    parentSplitNodeId = "fan",
                    parentJoinNodeId = "fan-in",
                    branchId = branch,
                )
            }
            store.admit(listOf(parent) + children)
            val waiting = parent.copy(
                state = ExecutionState.WaitingJoin("fan", "fan-in", 2, listOf("fan"), emptyList()),
                checkpointSeq = parent.checkpointSeq + 1,
            )
            assertTrue(repo.checkpoint(waiting, waiting.checkpointSeq, Instant.now(), null, Parking.Join))

            client.withConnection { c ->
                assertTrue(c.hasColumn("saga_join_barrier", "split_iteration"))
                c.prepareStatement("SELECT split_iteration FROM saga_join_branch WHERE parent_id = ?").use { ps ->
                    ps.setObject(1, parent.id)
                    ps.executeQuery().use { rs -> while (rs.next()) assertEquals(-1, rs.getInt(1)) }
                }
                c.prepareStatement("SELECT (waiting_state->>'splitIteration') IS NOT NULL FROM saga_execution WHERE id = ?").use { ps ->
                    ps.setObject(1, parent.id)
                    ps.executeQuery().use { rs -> rs.next(); assertFalse(rs.getBoolean(1), "same waiting_state shape as before") }
                }
            }

            val http = RecordingHttp { respond("{}", HttpStatusCode.OK, headersOf("Content-Type", "application/json")) }
            val executor = testExecutor(store, RecordingEnqueuer(), http.provider)

            // Branch a finishes on the new version and arrives on the old barrier.
            executor.execute(children[0])
            assertTrue(parent.id !in repo.findStalledJoinBarriers(1000), "one branch is still running")

            // Branch b finishes, but its arrival is lost: the scanner must still see the barrier.
            assertTrue(repo.finalizeCheckpointed(children[1].id, 0, "SUCCEEDED", null))
            assertTrue(parent.id in repo.findStalledJoinBarriers(1000))
            assertTrue(executor.resumeJoinIfSatisfied(parent.id))

            assertEquals("SUCCEEDED", repo.getExecutionStatus(parent.id)?.status)
            val join = repo.getStepResults(parent.id).single { it.stepName == "fan-in" }
            val joined = Json.parseToJsonElement(requireNotNull(join.responseBody)).jsonObject.getValue("branches").jsonArray
                .map { UUID.fromString(it.jsonObject.getValue("executionId").jsonPrimitive.content) }
            assertEquals(childIds.values.toSet(), joined.toSet())
        }
    }
}
