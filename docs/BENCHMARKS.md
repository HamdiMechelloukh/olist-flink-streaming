# Benchmarks

Measured throughput of the pipeline, how it saturates, and which operator is
responsible. Everything here is reproducible with `scripts/benchmark.py` on the
public [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) —
no private data, no vendor account.

## Headline

| Configuration | `orders` partitions | Job parallelism | Sustained throughput | Saturates at |
|---|---|---|---|---|
| **Old defaults** | 1 | 1 | **~500 events/s** | 1 000 events/s |
| Partitions only | 4 | 1 | ~500 events/s | 1 000 events/s |
| **Current defaults** | 4 | 4 | **2 000 events/s** | 4 000 events/s |

Two configuration values that had never been set — the partition count of the
auto-created topic and `parallelism.default` — separated 500 from 2 000 events
per second on identical hardware. Both are now set explicitly in
`docker/docker-compose.yml` (`KAFKA_NUM_PARTITIONS: 4` and
`parallelism.default: 4`), which is what these measurements prompted; the first
row records the behaviour before that change.

Raising partitions alone buys one step, not four: a single subtask still
consumes all four partitions, so only the fetch path benefits while processing
stays single-threaded.

## The bottleneck is not where the design notes said

The README's design note flags `RealtimeKpiJob`'s `windowAll` as the
single-partition bottleneck, "acceptable at this scale". The measurements
contradict that. Running all three jobs at once, on the old defaults
(1 partition, parallelism 1):

| Target rate | AnomalyDetection | RealtimeKpi | RevenueAggregation | Max lag | Drained |
|---|---|---|---|---|---|
| 100 /s | 1.7 % busy, 0 % bp | 1.5 % | 1.8 % | 0 | yes |
| 250 /s | 20.8 % busy, 8.1 % bp | 2.5 % | 3.6 % | 0 | yes |
| 500 /s | 31.5 % busy, 22.0 % bp | 2.4 % | 3.5 % | 4 941 | yes |
| 1 000 /s | **56.5 % busy, 54.1 % bp** | 2.3 % | 2.1 % | 9 170 | **no** |

`RealtimeKpiJob` never exceeds **2.5 % busy time and zero lag**, even at
saturation. The `windowAll` is not the constraint at this scale. The CEP
operator in `AnomalyDetectionJob` is: it is the only job that accumulates lag,
and the only one under back pressure.

The cause is in the pattern itself. "3+ orders from the same customer within 5
minutes" is keyed by customer, and the Olist dataset holds ~99 000 customers
who almost all placed a single order. The operator therefore maintains
five minutes of RocksDB state per customer to produce almost no matches.

Checkpoint duration is the same story seen from another angle:

| Target rate | AnomalyDetection | RealtimeKpi | RevenueAggregation |
|---|---|---|---|
| 100 /s | 183 ms | 47 ms | 160 ms |
| 250 /s | 3 851 ms | 63 ms | 234 ms |
| 500 /s | 3 954 ms | 151 ms | 172 ms |
| 1 000 /s | **5 830 ms** | 198 ms | 120 ms |

CEP state inflates its checkpoints 32-fold while the other two jobs stay under
250 ms.

## Isolating the bottleneck job

`AnomalyDetectionJob` run alone, so that the only variable is partitions and
parallelism. Three jobs at parallelism 4 would demand 12 slots; the
TaskManager offers 4, which is why the comparison isolates one job.

| Config | Partitions | Parallelism | Rate | Busy | Back pressure | Max lag | Checkpoint | Drained |
|---|---|---|---|---|---|---|---|---|
| B1 | 1 | 1 | 500 /s | 80.0 % | 67.5 % | 18 607 | 11 549 ms | no |
| B2 | 4 | 1 | 500 /s | 79.1 % | 62.2 % | 1 140 | 4 531 ms | yes |
| B2 | 4 | 1 | 1 000 /s | **100 %** | **99.9 %** | 40 382 | 32 682 ms | no |
| B3 | 4 | 4 | 500 /s | 72.7 % | 9.3 % | 0 | 6 728 ms | yes |
| B3 | 4 | 4 | 1 000 /s | 64.7 % | 31.2 % | 0 | 4 864 ms | yes |
| B3 | 4 | 4 | 2 000 /s | 84.7 % | 74.2 % | 7 001 | 13 950 ms | yes |
| B3 | 4 | 4 | 4 000 /s | 65.6 % | 56.0 % | 20 901 | 4 625 ms | no |

At parallelism 1 the TaskManager container stays pinned at roughly 100 % CPU —
one core — regardless of the four available slots and four host threads. At
parallelism 4 it spreads across all four. The 4× throughput gain matches the
core count, which is what makes the single-core ceiling structural rather than
a hardware limit.

## What would come next

- **Two-phase aggregation** is the fix the README proposes for `windowAll`, but
  the measurements put it low on the list: that operator is idle. The CEP
  operator is where the work is.
- **Reduce CEP state.** Keying five minutes of state per customer to catch
  repeat orders is expensive when nearly every customer appears once. Filtering
  to customers with a prior order, or a shorter window, would cut state
  directly.
- **Set the defaults.** Done: `KAFKA_NUM_PARTITIONS: 4` on the broker and
  `parallelism.default: 4` in `FLINK_PROPERTIES` are the two lines that unlock
  the 4× measured here.
- **Re-measure on a bigger host.** The 4× gain tracks the four cores of this
  machine. `scripts/benchmark.py` depends only on the standard library and
  `docker`, so the same ramp on more cores would show where the design — rather
  than the hardware — actually gives way.

## Method

`scripts/benchmark.py` drives the load and collects the measurements:

- **Load.** `OlistEventSimulator` runs in an `eclipse-temurin:21-jre`
  container on the compose network, at a target rate for a fixed duration.
- **Throughput is measured at the broker**, from `orders` end offsets before
  and after, divided by the sending duration — not self-reported by the
  generator. Measured rates matched targets exactly (45 001 messages for
  500 /s over 90 s).
- **Saturation criterion.** After each step the script waits up to 120 s for
  `pendingRecords` to fall back under 2 000. The ramp stops at the first step
  that does not drain. Lag that does not recover is the definition of
  saturation used here; a momentary spike is not.
- **Per-operator metrics** come from Flink's Prometheus reporter:
  `numRecordsInPerSecond`, `busyTimeMsPerSecond`,
  `backPressuredTimeMsPerSecond`, `operator_pendingRecords`, and the
  JobManager's `lastCheckpointDuration`. Reported values are averages over the
  steady state, discarding the first third of each step.

Run it with:

```bash
BENCH_RATES=500,1000,2000 BENCH_DURATION=90 python3 scripts/benchmark.py
```

### Honest limits of these numbers

- **Single node, modest host.** One TaskManager on an Intel i5-7400 (4 cores,
  4 threads) with 32 GB RAM, Docker CE inside WSL2. These figures show where
  *this topology on this machine* saturates and which operator gives way
  first. They are not a statement about Flink's capacity.
- **CPU percentages are indicative only.** `docker stats` reported up to 617 %
  for the TaskManager, which exceeds the 400 % ceiling of a 4-thread host, so
  instantaneous sampling overshoots under bursty load. Only the qualitative
  move from one core to four is claimed.
- **The saturation threshold is sensitive to how load is ramped.** 500 events/s
  drained during the gradual three-job ramp but not in the isolated run that
  started cold at 500, where the CEP operator builds its RocksDB state all at
  once. Read ~500 as a threshold, not a precise figure.
- **Iceberg disabled, checkpoints on local disk.** `ICEBERG_ENABLED=false` and
  checkpoints go to a container-local path rather than `s3a://`. MinIO
  withdrew its public container images, so the object-store path could not be
  exercised. This isolates stream processing from object-store latency, but
  the figures do not cover the Iceberg sink.
- **Replayed load.** Sustained runs replay the dataset, shifting event
  timestamps forward by one dataset span per pass so event time stays
  monotonic, and suffixing order and customer ids to keep keys distinct.
  Without the shift the watermark would already be ahead and every replayed
  event would be dropped as late.
