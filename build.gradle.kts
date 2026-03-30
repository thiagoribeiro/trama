import org.jetbrains.kotlin.gradle.dsl.JvmTarget

val ktorVersion = "3.4.0"
val kotlinxSerializationVersion = "1.7.3"
val msgpackVersion = "0.6.0"
val jooqVersion = "3.19.10"

plugins {
    kotlin("jvm") version "2.3.10"
    kotlin("plugin.serialization") version "2.3.10"
    application
    id("nu.studer.jooq") version "9.0"
}

group = "run.trama"
version = "0.1.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-call-logging:$ktorVersion")
    implementation("io.ktor:ktor-server-call-id:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages:$ktorVersion")
    implementation("io.ktor:ktor-server-compression:$ktorVersion")
    implementation("io.ktor:ktor-server-default-headers:$ktorVersion")
    implementation("io.ktor:ktor-server-forwarded-header:$ktorVersion")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.9.0")
    implementation("org.apache.commons:commons-pool2:2.12.0")
    implementation("com.ensarsarajcic.kotlinx:serialization-msgpack:$msgpackVersion")
    implementation("com.github.spullara.mustache.java:compiler:0.9.10")
    implementation("io.micrometer:micrometer-registry-prometheus:1.13.7")
    implementation("io.micrometer:micrometer-core:1.13.7")
    implementation("org.jooq:jooq:$jooqVersion")
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.liquibase:liquibase-core:4.29.2")
    implementation("com.sksamuel.hoplite:hoplite-core:2.8.1")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.8.1")
    implementation("io.opentelemetry:opentelemetry-api:1.43.0")
    implementation("io.opentelemetry:opentelemetry-sdk:1.43.0")
    implementation("io.opentelemetry:opentelemetry-sdk-trace:1.43.0")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.43.0")
    implementation("io.opentelemetry:opentelemetry-semconv:1.28.0-alpha")
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
    implementation("io.github.jamsesso:json-logic-java:1.0.7")

    jooqGenerator("org.jooq:jooq-meta:$jooqVersion")
    jooqGenerator("org.jooq:jooq-codegen:$jooqVersion")
    jooqGenerator("org.postgresql:postgresql:42.7.3")

    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("org.testcontainers:junit-jupiter:1.21.0")
    testImplementation("org.testcontainers:postgresql:1.21.0")
    testImplementation("io.ktor:ktor-client-mock:$ktorVersion")
    testImplementation("io.mockk:mockk:1.13.12")
    testImplementation("org.wiremock:wiremock-standalone:3.9.1")
}

jooq {
    version.set(jooqVersion)
    configurations {
        create("main") {
            generateSchemaSourceOnCompilation.set(false)
            jooqConfiguration.apply {
                jdbc.apply {
                    driver = "org.postgresql.Driver"
                    url = "jdbc:postgresql://${System.getenv("PGHOST") ?: "localhost"}:${System.getenv("PGPORT") ?: "5432"}/${System.getenv("PGDATABASE") ?: "saga"}"
                    user = System.getenv("PGUSER") ?: "saga"
                    password = System.getenv("PGPASSWORD") ?: "saga"
                }
                generator.apply {
                    name = "org.jooq.codegen.DefaultGenerator"
                    database.apply {
                        name = "org.jooq.meta.postgres.PostgresDatabase"
                        inputSchema = "public"
                    }
                    target.apply {
                        packageName = "com.example.jooq"
                        directory = "src/generated/jooq"
                    }
                }
            }
        }
    }
}

application {
    mainClass.set("run.trama.app.ApplicationKt")
}

tasks.register<JavaExec>("trama-validate") {
    group       = "application"
    description = "Validate a v2 saga definition offline (no orchestrator required)"
    classpath   = sourceSets.main.get().runtimeClasspath
    mainClass.set("run.trama.cli.ValidateCommandKt")
}

sourceSets {
    main {
        kotlin.srcDir("src/generated/jooq")
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
        freeCompilerArgs.addAll(
            "-Xjsr305=strict"
        )
    }
}

tasks.test {
    useJUnitPlatform()
    environment("DOCKER_HOST", "unix:///var/run/docker.sock")
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")
    systemProperty("api.version", "1.44")
}
