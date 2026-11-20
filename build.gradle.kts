import org.jetbrains.kotlin.gradle.dsl.JvmTarget

val ktorVersion = "3.6.1"
val kotlinxSerializationVersion = "1.9.0"
val msgpackVersion = "0.6.1"
val jooqVersion = "3.21.10"

plugins {
    kotlin("jvm") version "2.4.21"
    kotlin("plugin.serialization") version "2.4.21"
    application
    id("nu.studer.jooq") version "10.2.1"
}

group = "run.trama"
version = "3.1.0"

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
    implementation("io.ktor:ktor-client-okhttp:$ktorVersion")
    implementation("io.lettuce:lettuce-core:7.9.0.RELEASE")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.11.0")
    implementation("com.ensarsarajcic.kotlinx:serialization-msgpack:$msgpackVersion")
    implementation("com.github.spullara.mustache.java:compiler:0.9.14")
    implementation("io.micrometer:micrometer-registry-prometheus:1.18.0")
    implementation("io.micrometer:micrometer-core:1.18.0")
    implementation("org.jooq:jooq:$jooqVersion")
    implementation("org.postgresql:postgresql:42.7.14")
    implementation("com.zaxxer:HikariCP:7.1.0")
    implementation("org.liquibase:liquibase-core:4.33.0")
    implementation("com.sksamuel.hoplite:hoplite-core:3.0.7")
    implementation("com.sksamuel.hoplite:hoplite-yaml:3.0.7")
    implementation("io.opentelemetry:opentelemetry-api:1.68.0")
    implementation("io.opentelemetry:opentelemetry-sdk:1.68.0")
    implementation("io.opentelemetry:opentelemetry-sdk-trace:1.68.0")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.68.0")
    implementation("io.opentelemetry:opentelemetry-semconv:1.30.1-alpha")
    implementation("net.logstash.logback:logstash-logback-encoder:9.0")
    implementation("io.github.jamsesso:json-logic-java:1.1.0")

    jooqGenerator("org.jooq:jooq-meta:$jooqVersion")
    jooqGenerator("org.jooq:jooq-codegen:$jooqVersion")
    jooqGenerator("org.postgresql:postgresql:42.7.14")

    runtimeOnly("ch.qos.logback:logback-classic:1.6.8")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    testImplementation("org.testcontainers:testcontainers-postgresql:2.0.5")
    testImplementation("io.ktor:ktor-client-mock:$ktorVersion")
    testImplementation("io.mockk:mockk:1.14.12")
    testImplementation("org.wiremock:wiremock-standalone:3.13.2")
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
                        // Only generate the four parent tables — exclude all partition child
                        // tables (saga_execution_YYYYMM, etc.) and Liquibase metadata tables.
                        includes = "saga_execution|saga_step_result|saga_step_call|saga_definition"
                    }
                    target.apply {
                        packageName = "run.trama.jooq"
                        directory = "app/generated/jooq"
                    }
                }
            }
        }
    }
}

sourceSets {
    main {
        kotlin.srcDirs("app/main/kotlin", "app/generated/jooq")
        resources.srcDirs("app/main/resources")
    }
    test {
        kotlin.srcDirs("app/test/kotlin")
        resources.srcDirs("app/test/resources")
    }
}

application {
    mainClass.set("run.trama.app.ApplicationKt")
}

// Validation harness (loadtest/README.md): load generator, downstream mock, metrics collector
// and checker. Not part of `test`/CI; run with ./gradlew loadtest --args="<command> ...".
val loadtest: SourceSet by sourceSets.creating {
    kotlin.srcDir("app/loadtest/kotlin")
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
    runtimeClasspath += output + compileClasspath + sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("loadtest") {
    group       = "verification"
    description = "Validation harness: mock | drive | collect | check (see loadtest/README.md)"
    classpath   = loadtest.runtimeClasspath
    mainClass.set("run.trama.loadtest.MainKt")
    workingDir  = rootDir
}

// Writes the harness classpath so the loadtest shell scripts can start several JVMs quickly with `java -cp`.
tasks.register("loadtestClasspath") {
    group       = "verification"
    description = "Compile the validation harness and write its classpath to build/loadtest.classpath"
    dependsOn(loadtest.classesTaskName, "installDist")
    val out = layout.buildDirectory.file("loadtest.classpath")
    outputs.file(out)
    doLast { out.get().asFile.writeText(loadtest.runtimeClasspath.asPath) }
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

tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.addAll(listOf("-Xlint:-removal"))
}

tasks.test {
    useJUnitPlatform()
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock")
    environment("TESTCONTAINERS_RYUK_DISABLED", "true")
    systemProperty("api.version", "1.44")
}
