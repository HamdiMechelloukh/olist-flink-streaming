plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.0-beta12"
    jacoco
}

group = "com.olist.streaming"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

application {
    mainClass.set(project.findProperty("mainClass") as String? ?: "com.olist.streaming.jobs.RevenueAggregationJob")
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
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")

    // Kafka clients (for simulator producer)
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    // CSV parsing (simulator)
    implementation("org.apache.commons:commons-csv:1.12.0")

    // JSON serialization
    implementation("org.apache.flink:flink-json:$flinkVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Flink Table (needed for Iceberg RowData types)
    implementation("org.apache.flink:flink-table-common:$flinkVersion")

    // Iceberg + Flink
    implementation("org.apache.iceberg:iceberg-flink-runtime-2.0:$icebergVersion")

    // hadoop-common: needed by HadoopCatalog at runtime — bundled with aggressive exclusions
    implementation("org.apache.hadoop:hadoop-common:3.4.1") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
        exclude(group = "org.apache.zookeeper")
        exclude(group = "com.sun.jersey")
        exclude(group = "javax.servlet")
        exclude(group = "org.mortbay.jetty")
        exclude(group = "tomcat")
        exclude(group = "javax.ws.rs")
        exclude(group = "com.google.protobuf")
        exclude(group = "io.netty")
    }
    // hadoop-aws: provides S3AFileSystem — exclude bundle, provide individual SDK v1 modules
    implementation("org.apache.hadoop:hadoop-aws:3.4.1") {
        exclude(group = "com.amazonaws", module = "aws-java-sdk-bundle")
    }
    // AWS SDK v1 minimal modules required by S3AFileSystem (avoids the ~300 MB bundle)
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.780")
    implementation("com.amazonaws:aws-java-sdk-sts:1.12.780")

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
    jvmArgs("-Xmx2g")
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    isZip64 = true
}
