# olist-flink-streaming

[![CI](https://github.com/HamdiMechelloukh/olist-flink-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/HamdiMechelloukh/olist-flink-streaming/actions/workflows/ci.yml)

Real-time e-commerce analytics pipeline using Apache Flink, Kafka, and Iceberg, built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Architecture

```
Olist Event Simulator (Java)
        |
    [ Kafka: orders ]
        |
    [ Apache Flink ]
    ├── RevenueAggregationJob  -> [ Kafka: revenue-by-category ]
    ├── AnomalyDetectionJob    -> [ Kafka: order-alerts ]
    └── RealtimeKpiJob         -> [ Kafka: realtime-kpis ]
        |
    [ Apache Iceberg / MinIO ] (analytics storage)
```

## Jobs

| Job | Description | Window |
|-----|-------------|--------|
| **RevenueAggregationJob** | Revenue per product category | Tumbling 1 min |
| **AnomalyDetectionJob** | CEP: suspicious order frequency + price anomalies | CEP within 5 min |
| **RealtimeKpiJob** | Average order value, orders/min, total revenue | Tumbling 1 min |

## CEP Patterns

- **Suspicious Frequency**: 3+ orders from same customer within 5 minutes
- **Price Anomaly**: Order price exceeding threshold (> 500 BRL)

## Prerequisites

- Java 21
- Docker & Docker Compose
- Gradle (or use `./gradlew`)

## Quick Start

```bash
# Start infrastructure
docker compose -f docker/docker-compose.yml up -d

# Copy Olist CSVs to data/
cp /path/to/olist/*.csv data/

# Build
./gradlew build

# Run simulator
./gradlew run -PmainClass=com.olist.streaming.simulator.OlistEventSimulator

# Run jobs (in separate terminals)
./gradlew run -PmainClass=com.olist.streaming.jobs.RevenueAggregationJob
./gradlew run -PmainClass=com.olist.streaming.jobs.AnomalyDetectionJob
./gradlew run -PmainClass=com.olist.streaming.jobs.RealtimeKpiJob
```

## Testing

```bash
./gradlew test
```

Tests use Flink MiniCluster (no external infrastructure needed).

## Iceberg Sink

The Iceberg sink is optional, controlled by `ICEBERG_ENABLED=true`. When enabled, Flink jobs write results to Iceberg tables stored in MinIO (S3-compatible).

Initialize tables before first run:

```bash
./gradlew run -PmainClass=com.olist.streaming.sinks.IcebergTableInitializer
```

Tables created: `olist.order_events`, `olist.revenue_by_category`, `olist.realtime_kpis`.

### Why Iceberg over Delta Lake?

Delta Lake was considered but not retained for this project:

- **No Flink 2.0 connector**: Delta Lake's Flink connector (`delta-flink`) only supports Flink 1.x. No artifact exists for Flink 2.0 as of March 2026.
- **Iceberg has native Flink support**: `iceberg-flink-runtime-2.0` is officially maintained and published to Maven Central.
- **REST catalog**: Iceberg offers a standard REST catalog spec, making local setup with Docker straightforward (no dependency on Databricks Unity Catalog).
- **Broader engine compatibility**: Iceberg tables can be queried by Spark, Trino, Presto, and Flink without vendor lock-in.

## Stack

- **Apache Flink 2.0** — stream processing
- **Apache Kafka** — event streaming
- **Apache Iceberg** — analytics table format
- **MinIO** — S3-compatible object storage
- **Java 21** — language
- **Gradle** — build tool
- **JUnit 5 + Flink MiniCluster** — testing
