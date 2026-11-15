# olist-flink-streaming

[![CI](https://github.com/HamdiMechelloukh/olist-flink-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/HamdiMechelloukh/olist-flink-streaming/actions/workflows/ci.yml)

Real-time e-commerce analytics pipeline using Apache Flink, Kafka, and Iceberg, built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Architecture

```mermaid
flowchart TD
    SIM["🗂️ Olist Event Simulator\nCSV → Kafka producer (50 events/s)"]
    KAFKA_IN[["Kafka\norders"]]
    REV["RevenueAggregationJob\nTumbling 1 min · keyBy category"]
    ANOM["AnomalyDetectionJob\nCEP within 5 min · keyBy customer"]
    KPI["RealtimeKpiJob\nTumbling 1 min · windowAll"]
    KAFKA_REV[["Kafka\nrevenue-by-category"]]
    KAFKA_ALERT[["Kafka\norder-alerts"]]
    KAFKA_KPI[["Kafka\nrealtime-kpis"]]
    ICE[("Apache Iceberg\nSeaweedFS · s3a://warehouse")]
    CKP[("Flink Checkpoints\nRocksDB → s3a://")]

    SIM --> KAFKA_IN
    KAFKA_IN --> REV
    KAFKA_IN --> ANOM
    KAFKA_IN --> KPI
    REV --> KAFKA_REV
    ANOM --> KAFKA_ALERT
    KPI --> KAFKA_KPI
    REV -->|ICEBERG_ENABLED=true| ICE
    ANOM -->|ICEBERG_ENABLED=true| ICE
    KPI -->|ICEBERG_ENABLED=true| ICE
    REV -.->|checkpoint| CKP
    ANOM -.->|checkpoint| CKP
    KPI -.->|checkpoint| CKP
```

## Jobs

| Job | Description | Window |
|-----|-------------|--------|
| **RevenueAggregationJob** | Revenue per product category | Tumbling 1 min |
| **AnomalyDetectionJob** | CEP: suspicious order frequency + price anomalies | CEP within 5 min |
| **RealtimeKpiJob** | Average order value, orders/min, total revenue | Tumbling 1 min (global) |

## CEP Patterns

- **Suspicious Frequency**: 3+ orders from same customer within 5 minutes
- **Price Anomaly**: Order price exceeding threshold (> 500 BRL)

## Prerequisites

- Java 21
- Docker & Docker Compose
- Gradle (or use `./gradlew`)

## Configuration

Copy `.env.example` and adjust as needed:

```bash
cp .env.example .env
```

> **Note**: `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` are set to insecure
> defaults in `.env.example`. Change them before any non-local deployment.
>
> They keep their historical names because `IcebergCatalogConfig` reads them,
> but the object store is now SeaweedFS: MinIO withdrew its public container
> images, which stopped the stack from starting at all. SeaweedFS rejects signed
> requests unless an auth config exists, so the compose file generates one from
> these two variables at startup — they are the single source for the S3
> credentials.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `CHECKPOINT_INTERVAL_MS` | `30000` | Flink checkpoint interval in milliseconds |
| `ICEBERG_ENABLED` | `false` | Enable Iceberg sink on all jobs |
| `ICEBERG_CATALOG_URI` | `http://localhost:8181` | Iceberg REST catalog URL |
| `ICEBERG_WAREHOUSE` | `s3a://warehouse/` | Iceberg warehouse path |
| `ICEBERG_CATALOG_NAME` | `olist_catalog` | Iceberg REST catalog name |
| `MINIO_ROOT_USER` | `admin` | S3 access key |
| `MINIO_ROOT_PASSWORD` | `password123` | S3 secret key |
| `MINIO_ENDPOINT` | `http://localhost:9000` | S3 endpoint (SeaweedFS) |
| `OLIST_DATA_PATH` | `./data/` | Path to Olist CSV files |
| `EVENTS_PER_SECOND` | `50` | Simulator event throughput |
| `ORDERS_TOPIC` | `orders` | Kafka source topic for all jobs |
| `REVENUE_SINK_TOPIC` | `revenue-by-category` | Output topic for RevenueAggregationJob |
| `ALERTS_SINK_TOPIC` | `order-alerts` | Output topic for AnomalyDetectionJob |
| `KPI_SINK_TOPIC` | `realtime-kpis` | Output topic for RealtimeKpiJob |
| `SUSPICIOUS_ORDER_COUNT` | `3` | Min orders within 5 min to trigger a frequency alert |
| `PRICE_ANOMALY_THRESHOLD` | `500` | Order price (BRL) above which a price alert is fired |

## Quick Start (E2E)

The easiest way to see the pipeline in action is to use the provided E2E script. This will start the infrastructure, build the JAR, submit the jobs to the Flink cluster, and start the event simulator.

```bash
# 1. Clone the repository and copy .env
cp .env.example .env

# 2. Run the E2E script (requires Docker and Java 21)
./scripts/start-e2e.sh
```

## Screenshots

### Flink — 3 jobs RUNNING
![Flink Jobs](docs/screenshots/job-flink-running.png)

### Kafka — topics with live data
![Kafka Topics](docs/screenshots/flink-topics-kafka.png)

### Grafana — real-time metrics
![Grafana Dashboard](docs/screenshots/flink-grafana.png)

### Object store — Iceberg warehouse
![MinIO Warehouse](docs/screenshots/flink-s3-database.png)
![Iceberg Tables](docs/screenshots/flink-s3-olisttables.png)
![Table Content](docs/screenshots/flink-s3-table-content.png)

## UI Access

Once the pipeline is running, you can access the following UIs:

| UI | URL | Description |
|----|-----|-------------|
| **Flink Web UI** | [http://localhost:8081](http://localhost:8081) | Job graph, task metrics, and checkpoints. |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | Inspect messages in Kafka topics. |
| **Grafana** | [http://localhost:3001](http://localhost:3001) | Real-time dashboards (Admin: `admin`/`admin`). |
| **SeaweedFS Filer** | [http://localhost:8888](http://localhost:8888) | Browse the S3-compatible store holding Iceberg tables and checkpoints. |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | Raw metric queries. |

## Manual Operations

If you prefer to run steps manually:

### 1. Infrastructure
```bash
docker compose -f docker/docker-compose.yml up -d
```

### 2. Build and Submit
```bash
./gradlew clean shadowJar
docker cp build/libs/olist-flink-streaming-1.0-SNAPSHOT.jar docker-jobmanager-1:/tmp/job.jar
docker exec -d docker-jobmanager-1 flink run -c com.olist.streaming.jobs.RevenueAggregationJob /tmp/job.jar
```

### 3. Run Simulator (Host)
```bash
export $(grep -v '^#' .env | xargs)
java -cp build/libs/olist-flink-streaming-1.0-SNAPSHOT.jar com.olist.streaming.simulator.OlistEventSimulator
```

## Testing

```bash
./gradlew test
```

Tests use Flink MiniCluster (no external infrastructure needed).

## Iceberg Sink

The Iceberg sink is optional, controlled by `ICEBERG_ENABLED=true`. When enabled, Flink jobs write results to Iceberg tables stored in SeaweedFS (S3-compatible).

Initialize tables before first run (handled automatically by `start-e2e.sh`):

```bash
docker exec docker-jobmanager-1 flink run -c com.olist.streaming.sinks.IcebergTableInitializer /tmp/job.jar
```

Tables created: `olist.revenue_by_category`, `olist.realtime_kpis`, `olist.order_alerts`. The `revenue_by_category` table is partitioned by day on `window_start`, reducing scan cost for time-range queries.

### Why Iceberg over Delta Lake?

Delta Lake was considered but not retained for this project:

- **No Flink 2.0 connector**: Delta Lake's Flink connector (`delta-flink`) only supports Flink 1.x. No artifact exists for Flink 2.0 as of March 2026.
- **Iceberg has native Flink support**: `iceberg-flink-runtime-2.0` is officially maintained and published to Maven Central.
- **REST catalog**: Iceberg offers a standard REST catalog spec, making local setup with Docker straightforward (no dependency on Databricks Unity Catalog).
- **Broader engine compatibility**: Iceberg tables can be queried by Spark, Trino, Presto, and Flink without vendor lock-in.

## Observability

Each job exposes custom Flink metrics visible in the Flink Web UI under Task Metrics:

| Metric | Job | Description |
|--------|-----|-------------|
| `windowsEmitted` | RevenueAggregationJob | Number of revenue windows produced |
| `kpiWindowsEmitted` | RealtimeKpiJob | Number of KPI windows produced |
| `priceAnomalyAlertsEmitted` | AnomalyDetectionJob | Price anomaly alerts fired |
| `suspiciousFrequencyAlertsEmitted` | AnomalyDetectionJob | Suspicious frequency alerts fired |

## Benchmarks

Measured on a single node (Intel i5-7400, 4 threads, Docker CE in WSL2), on the
public Olist dataset. Full method, per-operator figures and limits in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Configuration | `orders` partitions | Parallelism | Sustained | Saturates at |
|---|---|---|---|---|
| **Old defaults** | 1 | 1 | **~500 events/s** | 1 000 events/s |
| Partitions only | 4 | 1 | ~500 events/s | 1 000 events/s |
| **Current defaults** | 4 | 4 | **2 000 events/s** | 4 000 events/s |

Two values that had never been set — the partition count of the auto-created
topic and `parallelism.default` — separated 500 from 2 000 events per second on
identical hardware. Both are now set explicitly, which is what these
measurements prompted. At parallelism 1 the TaskManager stayed pinned to one
core whatever the four available slots suggested. Raising partitions alone buys
one step, not four: a single subtask still consumes every partition.

**The bottleneck is the CEP operator, not `windowAll`.** At saturation
`AnomalyDetectionJob` sits at 56 % busy time with 54 % back pressure and is the
only job accumulating lag, while `RealtimeKpiJob` never exceeds 2.5 % busy time
with zero lag. Its checkpoints grow from 183 ms to 5.8 s as CEP state builds —
the pattern keys five minutes of state per customer, and nearly every Olist
customer placed a single order.

Throughput is measured at the broker from `orders` end offsets, not
self-reported by the load generator:

```bash
BENCH_RATES=500,1000,2000 BENCH_DURATION=90 python3 scripts/benchmark.py
```

## Design Notes

- **`RealtimeKpiJob` uses `windowAll`**: global KPIs (total orders, total revenue) require aggregating across all partitions. `windowAll` is intentional here — the single-partition bottleneck is acceptable at this scale. For higher throughput, a two-phase aggregation (pre-aggregate per key, then merge) would be needed. **Measured correction**: benchmarking shows this operator is *not* the constraint at this scale — it stays under 2.5 % busy time with zero lag even at saturation, while the CEP operator in `AnomalyDetectionJob` saturates first. Two-phase aggregation would be the right fix if `windowAll` were the limit, but the measurements put it low on the list. See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Known Limitations & Future Work

- **State TTL**: The current CEP patterns use `within()` for time-bounding, which is sufficient here. If `KeyedProcessFunction` with manual state were introduced (e.g., for deduplication or enrichment), explicit [State TTL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl) configuration would be required to prevent unbounded state growth.
- **Schema Registry**: Events are currently serialized as JSON without schema enforcement. In a production pipeline, using a [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) with Avro or Protobuf would add schema evolution guarantees and catch producer/consumer contract mismatches before they reach the stream processor.
- **Simulator tests**: The `OlistEventSimulator` is not covered by unit tests as it requires external CSV files and a Kafka broker. It is validated by the end-to-end run.

## Stack

- **Apache Flink 2.0** — stream processing
- **Apache Kafka** — event streaming
- **Apache Iceberg** — analytics table format
- **SeaweedFS** — S3-compatible object storage
- **Java 21** — language
- **Gradle** — build tool
- **JUnit 5 + Flink MiniCluster** — testing
