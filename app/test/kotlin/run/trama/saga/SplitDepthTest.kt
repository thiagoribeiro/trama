package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SplitDepthTest {
    @Test
    fun `no header is depth 0, a non-negative integer is that depth, anything else is invalid`() {
        assertEquals(0, SplitDepth.parseHeader(null))
        assertEquals(0, SplitDepth.parseHeader("0"))
        assertEquals(7, SplitDepth.parseHeader(" 7 "))
        listOf("", " ", "-1", "1.5", "abc", "0x2", "99999999999").forEach {
            assertNull(SplitDepth.parseHeader(it), "'$it' must be rejected")
        }
    }
}
