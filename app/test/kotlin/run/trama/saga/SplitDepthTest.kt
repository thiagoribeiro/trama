package run.trama.saga

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SplitDepthTest {
    @Test
    fun `a start request without the header begins at depth 0`() {
        assertEquals(0, SplitDepth.parseHeader(null))
    }

    @Test
    fun `a non-negative integer header is the start depth`() {
        assertEquals(0, SplitDepth.parseHeader("0"))
        assertEquals(2, SplitDepth.parseHeader("2"))
        assertEquals(7, SplitDepth.parseHeader(" 7 "))
    }

    @Test
    fun `anything else is invalid`() {
        listOf("", " ", "-1", "+1", "1.5", "abc", "1e3", "0x2", "99999999999").forEach {
            assertNull(SplitDepth.parseHeader(it), "'$it' must be rejected")
        }
    }

    @Test
    fun `a split may spawn branches up to maxDepth`() {
        assertNull(SplitDepth.exceeded(parentDepth = 2, maxDepth = 3))
        assertEquals("split depth 4 exceeds split.maxDepth 3", SplitDepth.exceeded(parentDepth = 3, maxDepth = 3))
        assertEquals("split depth 1 exceeds split.maxDepth 0", SplitDepth.exceeded(parentDepth = 0, maxDepth = 0))
        assertEquals("split depth 2147483648 exceeds split.maxDepth 3", SplitDepth.exceeded(Int.MAX_VALUE, 3))
    }
}
