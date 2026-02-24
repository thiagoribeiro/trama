package io.trama.saga

import io.mockk.every
import io.mockk.mockk
import io.trama.saga.store.DatabaseClient
import io.trama.saga.store.SagaRepository
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
class SagaDefinitionCrudTest {
    @Test
    fun `getDefinition reads from cache when available`() {
        val db = mockk<DatabaseClient>(relaxed = true)
        every { db.withConnection<Int>(any()) } returns 1
        val repo = SagaRepository(db)
        val id = UUID.randomUUID()
        repo.insertDefinition(id, "s1", "1", "{}")
        val result = repo.getDefinition(id)
        assertNotNull(result)
        assertEquals("s1", result.name)
    }
}
