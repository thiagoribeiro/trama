package run.trama.runtime

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import run.trama.config.JoinCompletionScannerConfig

class JoinCompletionScannerTest {

    private class FixedRepo(val ids: List<UUID>) : JoinBarrierRepository {
        var requestedLimit: Int? = null
        override suspend fun findStalledJoinBarriers(limit: Int): List<UUID> {
            requestedLimit = limit
            return ids
        }
    }

    private class ScriptedResumer(val behavior: (UUID) -> Boolean) : JoinResumer {
        val attempted = mutableListOf<UUID>()
        override suspend fun resumeJoinIfSatisfied(parentId: UUID): Boolean {
            attempted += parentId
            return behavior(parentId)
        }
    }

    private val config = JoinCompletionScannerConfig(batchSize = 7)

    @Test
    fun `counts only joins that were actually resumed`() = runBlocking<Unit> {
        val ids = List(3) { UUID.randomUUID() }
        val repo = FixedRepo(ids)
        val resumer = ScriptedResumer { it != ids[1] }

        val resumed = JoinCompletionScanner(repo, resumer, config).scan()

        assertEquals(2, resumed)
        assertEquals(ids, resumer.attempted)
        assertEquals(7, repo.requestedLimit, "batch size is forwarded to the repository")
    }

    @Test
    fun `an exception for one parent does not stop the others`() = runBlocking<Unit> {
        val ids = List(3) { UUID.randomUUID() }
        val resumer = ScriptedResumer { if (it == ids[0]) error("boom") else true }

        val resumed = JoinCompletionScanner(FixedRepo(ids), resumer, config).scan()

        assertEquals(2, resumed)
        assertEquals(ids, resumer.attempted)
    }

    @Test
    fun `nothing stalled resumes nothing`() = runBlocking<Unit> {
        val resumer = ScriptedResumer { true }
        assertEquals(0, JoinCompletionScanner(FixedRepo(emptyList()), resumer, config).scan())
        assertEquals(emptyList(), resumer.attempted)
    }
}
