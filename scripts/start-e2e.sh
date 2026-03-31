#!/bin/bash
set -e

# Olist Flink Streaming - End-to-End Starter Script
# This script automates the setup and execution of the streaming pipeline for evaluation.

echo "🚀 Starting Olist Flink Streaming Pipeline..."

# 1. Start Infrastructure
echo "🐳 Starting Docker containers..."
docker compose --env-file .env -f docker/docker-compose.yml up -d

# Wait for JobManager to be ready
echo "⏳ Waiting for Flink JobManager to be healthy..."
until curl -sf http://localhost:8081/overview > /dev/null; do
  sleep 2
done
echo "✅ JobManager is up!"

# 2. Build the Application
echo "🏗️ Building Shadow JAR..."
./gradlew shadowJar

# 3. Copy JAR to Flink container
JAR_PATH="build/libs/olist-flink-streaming-1.0-SNAPSHOT.jar"
echo "📦 Copying JAR to Flink container..."
docker cp "$JAR_PATH" docker-jobmanager-1:/tmp/job.jar

# 4. Initialize Iceberg Tables inside the Flink container (Hadoop/S3 plugin available there)
echo "🧊 Initializing Iceberg tables in MinIO..."
docker exec -e MINIO_ENDPOINT=http://minio:9000 \
            -e MINIO_ROOT_USER="${MINIO_ROOT_USER:-admin}" \
            -e MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password123}" \
  docker-jobmanager-1 flink run -c com.olist.streaming.sinks.IcebergTableInitializer /tmp/job.jar \
  || echo "⚠️ Table initialization skipped or already exists."

echo "📤 Submitting jobs to Flink cluster..."

# Submit jobs to the cluster
docker exec -d docker-jobmanager-1 flink run -c com.olist.streaming.jobs.RevenueAggregationJob /tmp/job.jar
docker exec -d docker-jobmanager-1 flink run -c com.olist.streaming.jobs.AnomalyDetectionJob /tmp/job.jar
docker exec -d docker-jobmanager-1 flink run -c com.olist.streaming.jobs.RealtimeKpiJob /tmp/job.jar

echo "✅ Flink jobs submitted! View them at http://localhost:8081"

# 5. Run Simulator
echo "📈 Starting Event Simulator..."
echo "💡 The simulator will now send events to Kafka at 50 events/sec."
echo "💡 Press Ctrl+C to stop the simulator (this won't stop the Flink jobs or Docker)."

# Source .env safely
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

java -cp "$JAR_PATH" com.olist.streaming.simulator.OlistEventSimulator
