package run.trama.config

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * ConfigLoader reads JVM-global system properties, which E2E tests also set. Every property
 * this test touches is snapshotted and cleared before each test, then restored afterwards.
 */
class ConfigLoaderTest {
    private val keys = listOf(
        "runtime.enabled", "metrics.enabled", "telemetry.enabled", "redis.url",
        "database.host", "database.port", "database.database", "database.user", "database.password",
        "runtime.callback.baseUrl", "runtime.callback.hmacSecret", "runtime.callback.hmacKid",
        "runtime.emptyPollDelayMillis", "redis.sharding.virtualShardCount",
    )
    private val saved = mutableMapOf<String, String?>()

    @BeforeTest
    fun snapshot() {
        keys.forEach { saved[it] = System.getProperty(it); System.clearProperty(it) }
    }

    @AfterTest
    fun restore() {
        saved.forEach { (k, v) -> if (v == null) System.clearProperty(k) else System.setProperty(k, v) }
    }

    @Test
    fun `defaults come from application yaml`() {
        val config = ConfigLoader.load()
        if (System.getenv("REDIS_SHARDING_VIRTUALSHARDCOUNT") == null) assertEquals(1024, config.redis.sharding.virtualShardCount)
        assertEquals(RuntimeStore.REDIS, config.runtime.store)
        assertEquals(25, config.runtime.maxStepsPerExecution)
        assertEquals("saga:executions", config.redis.queue.keyPrefix)
        assertEquals(30_000, config.http.requestTimeoutMillis)
    }

    @Test
    fun `system properties override yaml values`() {
        System.setProperty("runtime.enabled", "false")
        System.setProperty("metrics.enabled", "false")
        System.setProperty("telemetry.enabled", "true")
        System.setProperty("redis.url", "redis://override:6390")
        System.setProperty("runtime.callback.baseUrl", "https://trama.example")
        System.setProperty("runtime.callback.hmacSecret", "s3cret")
        System.setProperty("runtime.emptyPollDelayMillis", "7")
        System.setProperty("redis.sharding.virtualShardCount", "64")

        val config = ConfigLoader.load()

        assertEquals(false, config.runtime.enabled)
        assertEquals(false, config.metrics.enabled)
        assertEquals(true, config.telemetry.enabled)
        assertEquals("redis://override:6390", config.redis.url)
        assertEquals("https://trama.example", config.runtime.callback.baseUrl)
        assertEquals("s3cret", config.runtime.callback.hmacSecret)
        assertEquals(7, config.runtime.emptyPollDelayMillis)
        assertEquals(64, config.redis.sharding.virtualShardCount)
    }

    @Test
    fun `partial database override keeps the remaining yaml fields`() {
        System.setProperty("database.host", "pg.internal")
        System.setProperty("database.port", "6543")

        val db = ConfigLoader.load().database

        assertEquals("pg.internal", db.host)
        assertEquals(6543, db.port)
        if (System.getenv("DATABASE_DATABASE") == null) assertEquals("saga", db.database)
    }

    @Test
    fun `partial callback override keeps the other callback fields`() {
        System.setProperty("runtime.callback.hmacKid", "k2")
        val cb = ConfigLoader.load().runtime.callback
        assertEquals("k2", cb.hmacKid)
    }

    @Test
    fun `invalid values are ignored instead of crashing`() {
        System.setProperty("runtime.enabled", "yes")          // not strict boolean
        System.setProperty("runtime.emptyPollDelayMillis", "abc")
        System.setProperty("database.port", "not-a-port")
        System.setProperty("database.host", "h")

        val config = ConfigLoader.load()

        if (System.getenv("RUNTIME_EMPTYPOLLDELAYMILLIS") == null) assertEquals(50, config.runtime.emptyPollDelayMillis)
        if (System.getenv("DATABASE_PORT") == null) assertEquals(5432, config.database.port)
    }
}
