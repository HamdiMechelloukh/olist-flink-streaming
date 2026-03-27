plugins {
    java
    application
}

group = "com.olist.streaming"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

val flinkVersion = "2.0.0"
val kafkaConnectorVersion = "4.0.1-2.0"
val icebergVersion = "1.10.1"
val jacksonVersion = "2.17.2"
val junitVersion = "5.11.4"

repositories {
    mavenCentral()
}

dependencies {
    // Flink core
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")

    // Flink CEP
    implementation("org.apache.flink:flink-cep:$flinkVersion")

    // Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")

    // Kafka clients (for simulator producer)
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    // JSON serialization
    implementation("org.apache.flink:flink-json:$flinkVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Flink Table (needed for Iceberg RowData types)
    implementation("org.apache.flink:flink-table-common:$flinkVersion")

    // Iceberg + Flink
    implementation("org.apache.iceberg:iceberg-flink-runtime-2.0:$icebergVersion")

    // Hadoop (required by Iceberg CatalogLoader)
    implementation("org.apache.hadoop:hadoop-common:3.4.1") {
        exclude(group = "org.slf4j")
    }

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.24.3")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.24.3")

    // Flink runtime (provided in cluster, needed for local)
    runtimeOnly("org.apache.flink:flink-runtime-web:$flinkVersion")

    // Test
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "com.olist.streaming.jobs.RevenueAggregationJob"
    }
    // Fat JAR for Flink submission
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    isZip64 = true
}
