# Streaming Benchmark — Implementation Plan

Second benchmark scenario focused exclusively on **stateful streaming results**. Where benchmark 1 measured query latency against a pre-ingested dataset, benchmark 2 measures how fast each system can produce and continuously update a stateful result relative to the live producer.

---

## Goal

Compare three systems on a single axis: **how quickly does a stateful result reflect the latest data the producer has written to Kafka?**

This isolates the push/pull architectural difference under equal hardware and production-grade configuration.

---

## Systems Under Test

| System | Model | Mechanism |
|---|---|---|
| **Flink** | Push | Continuous streaming SQL emits updated results to Kafka result topics as each event changes state |
| **ClickHouse** | Pull (micro-batch) | Kafka Engine + AggregatingMergeTree Materialized View; maintained aggregate table polled by probe |
| **StarRocks** | Pull (micro-batch) | Routine Load → native PK table; probe re-queries on each tick |

Fluss is excluded — it is a storage layer, not a stateful processing engine. All systems read directly from Kafka.

---

## Resource Constraints

Equal CPU and memory for each system under test. Production-grade allocations — no memory-only modes, no disabled safety features.

| Container | CPUs | Memory | Notes |
|---|---|---|---|
| `flink-taskmanager` | 4 | 4 GB | Data plane only; JM is control plane overhead |
| `clickhouse` | 4 | 4 GB | Up from 2 CPU / 2 GB |
| `starrocks` | 4 | 4 GB | Up from 2 CPU / 4 GB (CPU equalized) |
| All others | unchanged | unchanged | Kafka, ZK, Fluss, generator, probe |

---

## Production-Grade Configuration

### Flink
| Setting | Value | Reason |
|---|---|---|
| `state.backend` | `rocksdb` | Required for production stateful jobs; handles state beyond heap |
| `state.backend.incremental` | `true` | Incremental RocksDB checkpoints — smaller, faster |
| `execution.checkpointing.interval` | `30s` | Failure recovery guarantee; overhead is real and included in measurement |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | Production standard |
| `execution.checkpointing.min-pause` | `10s` | Prevents checkpoint storms |
| `execution.checkpointing.tolerable-failed-checkpoints` | `3` | Tolerates transient failures |
| `execution.buffer-timeout` | `1ms` | Legitimate low-latency production setting; flushes network buffers near-immediately |
| `parallelism.default` | `2` | Saturates 4 CPUs across source + operator chains |

### ClickHouse
| Setting | Value | Reason |
|---|---|---|
| `kafka_flush_interval_ms` | `500` | Production floor for low-latency ingestion; 100ms causes part explosion |
| `kafka_num_consumers` | `4` | Parallelizes consumption across 12 topic partitions |
| `kafka_max_block_size` | `65536` | Batch size per consumer poll |
| Table engine | `AggregatingMergeTree` | Correct production pattern for maintained aggregates; partial states merged in background |

### StarRocks
| Setting | Value | Reason |
|---|---|---|
| `max_batch_interval` | `1` | Minimum scheduler cadence (hard floor) |
| `desired_concurrent_number` | `4` | One task per 3 partitions (12 partitions total) |
| `max_batch_rows` | `100000` | Commit when 100K rows consumed per task |
| `strict_mode` | `false` | Matches existing P6 setup; avoids schema mismatch halts |

---

## The Three Queries

All three are implemented on all three systems. Each is designed to benchmark a distinct aspect of stateful streaming.

### Q-B — Running Total per Region (Unbounded State)

```sql
SELECT region, COUNT(*), SUM(amount), MAX(event_time)
FROM transactions
GROUP BY region
```

**What it benchmarks:** Pure update frequency. State is tiny (7 region keys), never evicted. Isolates the ingestion-to-result pipeline latency with no state management overhead. The cleanest push vs pull comparison.

**Expected structural floor:**
- Flink: ~1–10ms (buffer-timeout)
- ClickHouse: ~500ms (kafka_flush_interval_ms)
- StarRocks: ~1s (max_batch_interval minimum)

**Per-system implementation:**
- **Flink:** `GROUP BY region` continuous aggregation → `upsert-kafka` sink keyed by region. Emits a changelog record per region on every state change.
- **ClickHouse:** `AggregatingMergeTree` table `streaming_qb` maintained by MV from `kafka_transactions_s`. Probe reads `max(max_event_time)` from the aggregate table.
- **StarRocks:** Probe runs `SELECT MAX(event_time) FROM transactions_s` directly against the Routine Load target table.

---

### Q-A — Tumbling 1-Minute Window

```sql
SELECT window_start, region, COUNT(*), SUM(amount), MAX(event_time)
FROM transactions
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), region
```

**What it benchmarks:** Window lifecycle management — open, accumulate, close, emit. Two sub-metrics: (1) intra-window freshness (how current is the open window's partial result?), (2) finalization lag (how quickly after a minute boundary does each system produce a final result for the closed window?).

Flink has an explicit window close event (driven by processing time watermark). ClickHouse and StarRocks have no finalization signal — their per-minute bucket just stops updating.

**Per-system implementation:**
- **Flink:** `TUMBLE(proc_time, INTERVAL '1' MINUTE)` window aggregate → `upsert-kafka` sink keyed by `(window_start, region)`. Emits the final result when the window closes.
- **ClickHouse:** `AggregatingMergeTree` table `streaming_qa` grouped by `toStartOfMinute(event_time)` + region. No explicit close. Probe reads `max(max_event_time)` for the current and previous minute bucket.
- **StarRocks:** Probe runs `SELECT MAX(event_time) FROM transactions_s WHERE event_time >= DATE_TRUNC('minute', NOW())` — re-aggregates on each tick.

**Probe measurement for Q-A:** Two measurements per tick:
1. **Intra-window lag** — `kafka_latest_event_time - MAX(event_time in current open window result)`
2. **Finalization lag** (recorded at each minute boundary) — `time_result_appeared - window_end_time`

---

### Q-C — Last 5 Minutes Sliding Aggregate

```sql
SELECT region, COUNT(*), SUM(amount), MAX(event_time)
FROM transactions
WHERE event_time >= NOW() - INTERVAL '5' MINUTE
GROUP BY region
```

**What it benchmarks:** State eviction and recomputation cost. This is where the push/pull difference is most consequential:
- Flink maintains incremental sliding window state (OVER window); cost is O(1) per event regardless of data volume.
- ClickHouse and StarRocks re-scan the raw events table on every probe tick; cost is O(rows in 5-minute window), growing over time.

**Per-system implementation:**
- **Flink:** OVER window aggregation partitioned by region, ordered by `proc_time`, 5-minute range. Emits one result per input event → `upsert-kafka` sink keyed by region. Compaction retains latest per region.
- **ClickHouse:** No maintained state. Probe re-scans `streaming_transactions` (raw MergeTree) with `WHERE event_time >= now() - INTERVAL 5 MINUTE GROUP BY region`.
- **StarRocks:** No maintained state. Probe re-runs `SELECT MAX(event_time) FROM transactions_s WHERE event_time >= NOW() - INTERVAL '5' MINUTE`.

---

## Measurement Methodology

### Source of Truth: Kafka

`kafka_latest_event_time` = MAX(event_time) across the last message on each partition of the `transactions` topic. This reflects exactly what the producer has written, independent of wall clock.

### Primary Metric

```
system_lag_ms = kafka_latest_event_time - MAX(event_time in system result)
```

Measures purely how far behind the producer each system's maintained result is. Producer slowdowns do not inflate this number.

### Secondary Metrics

| Metric | Formula | Captured for |
|---|---|---|
| `producer_lag_ms` | `NOW() - kafka_latest_event_time` | All — shows if producer itself is behind |
| `probe_duration_ms` | Wall time of each system probe query | All — separates query overhead from staleness |
| `finalization_lag_ms` | `time_result_appeared - window_end_time` | Q-A only — per-minute event |

### Probe Cadence

500ms polling interval. Runs continuously as a Docker service. Exposes Prometheus metrics on `:8000`. Prometheus scrapes every 2s. Grafana visualizes `system_lag_ms` time series per system × query.

---

## Architecture

```
Kafka topic: transactions (12 partitions)
        │
        ├──► Flink (consumer group: flink-streaming-benchmark)
        │         │  continuous streaming SQL (Q-A, Q-B, Q-C in one STATEMENT SET)
        │         ▼
        │    flink_result_qa / qb / qc  (Kafka compacted topics)
        │                │
        │                ▼ probe reads latest per region
        │
        ├──► ClickHouse (consumer group: ch-streaming)
        │         │  kafka_flush_interval_ms=500, kafka_num_consumers=4
        │         ▼
        │    kafka_transactions_s (Kafka Engine)
        │         │  MVs
        │         ├──► streaming_transactions (MergeTree, for Q-C scans)
        │         ├──► streaming_qb (AggregatingMergeTree, running totals)
        │         └──► streaming_qa (AggregatingMergeTree, per-minute windows)
        │                │
        │                ▼ probe queries
        │
        └──► StarRocks (consumer group: starrocks-streaming)
                  │  Routine Load, max_batch_interval=1, desired_concurrent_number=4
                  ▼
             transactions_s (PK table)
                  │
                  ▼ probe re-queries Q-A / Q-B / Q-C on each tick
```

---

## Files to Create / Modify

### New files

| File | Purpose |
|---|---|
| `docker/clickhouse/init/03_streaming_benchmark.sql` | Kafka Engine + MergeTree + AggregatingMergeTree tables + MVs for Q-A, Q-B |
| `docker/starrocks/init/04_streaming_benchmark.sql` | `transactions_s` table + tuned Routine Load |
| `docker/flink/sql/06_streaming_benchmark.sql` | Kafka source + 3 result sinks + STATEMENT SET (Q-A, Q-B, Q-C) |
| `probe/probe.py` | 500ms probe loop: Kafka source-of-truth + all 3 systems × 3 queries → Prometheus metrics |
| `probe/requirements.txt` | confluent-kafka, clickhouse-connect, pymysql, prometheus-client |
| `probe/Dockerfile` | Python 3.12 slim image |

### Modified files

| File | Change |
|---|---|
| `docker-compose.yml` | (1) ClickHouse: 2→4 CPUs, 2G→4G RAM. (2) StarRocks: 2→4 CPUs. (3) Flink TM: 2→4 CPUs. (4) `kafka-init`: create `flink_result_qa/qb/qc` compacted topics. (5) Add `probe` service. |
| `docker/prometheus/prometheus.yml` | Add `probe:8000` scrape target (2s interval) |
| `Makefile` | Add streaming benchmark targets (below) |

---

## Kafka Result Topics (Flink sinks)

| Topic | Partitions | Cleanup | Key | Value |
|---|---|---|---|---|
| `flink_result_qb` | 4 | compact | `{"region":"..."}` | `{region, event_count, total_amount, max_event_time}` |
| `flink_result_qa` | 4 | compact | `{"window_start":"...","region":"..."}` | `{window_start, region, event_count, total_amount, max_event_time}` |
| `flink_result_qc` | 4 | compact | `{"region":"..."}` | `{region, event_count, total_amount, max_event_time}` |

Compact cleanup ensures the probe always sees the latest result per key without reading full history.

---

## Probe Logic (probe/probe.py)

```
Every 500ms:
  1. kafka_latest = seek to last offset on each partition of 'transactions', read + parse MAX(event_time)
  2. For each query in [qa, qb, qc]:
       a. Flink   → read latest N messages from flink_result_{query} topic → MAX(max_event_time)
       b. CH      → SQL query against streaming_qa / streaming_qb / streaming_transactions
       c. SR      → SQL query against transactions_s
       d. system_lag_ms = kafka_latest_ms - result_max_event_time_ms
       e. emit Prometheus gauge: streaming_system_lag_ms{system, query}
       f. record probe_duration_ms{system, query}
  3. emit streaming_kafka_latest_event_time_ms
  4. sleep until next 500ms tick
```

### Prometheus metrics exposed

| Metric | Labels | Description |
|---|---|---|
| `streaming_kafka_latest_event_time_ms` | — | Epoch ms of latest Kafka event_time |
| `streaming_system_lag_ms` | `system`, `query` | Primary metric: ms behind Kafka |
| `streaming_producer_lag_ms` | — | `NOW() - kafka_latest_event_time` |
| `streaming_probe_duration_seconds` | `system`, `query` | How long each probe query took |

---

## Makefile Targets (to add)

```makefile
# Streaming benchmark — preparation
stream-init          # create SR table + start Routine Load (run once)
stream-flink-submit  # submit 06_streaming_benchmark.sql (cancel others first)
stream-cancel        # cancel Flink streaming benchmark job

# Streaming benchmark — operation
stream-probe-logs    # follow probe container logs
stream-sr-status     # show transactions_s Routine Load state
stream-ch-status     # show ClickHouse kafka_transactions_s consumer status

# Open Grafana at streaming dashboard
stream-grafana       # open http://localhost:3000 (streaming benchmark dashboard)
```

---

## Operational Notes

### Before starting
1. `make flink-cancel` — cancel existing Flink jobs (Kafka→Fluss, P5) to free task slots for the streaming benchmark. The streaming benchmark needs ~6 slots (parallelism=2 × 3 queries).
2. `PAUSE ROUTINE LOAD FOR analytics.transactions_p6_load` — avoid concurrent ingestion contention on the single-BE StarRocks node (same issue as P5 benchmark).
3. `make stream-init` — creates `transactions_s` table and starts the Routine Load.
4. `make stream-flink-submit` — submits the Flink streaming job.
5. Let all three systems reach steady state (~60s) before reading probe metrics.

### Consumer groups (isolated from benchmark 1)

| System | Consumer group | Benchmark 1 group |
|---|---|---|
| Flink | `flink-streaming-benchmark` | `flink-fluss-consumer` |
| ClickHouse | `ch-streaming` | `clickhouse-consumer` |
| StarRocks | `starrocks-streaming` | `starrocks-routine-load` |

All are independent — running the streaming benchmark does not affect benchmark 1 consumer offsets.

---

## Grafana Dashboard

New dashboard: **Streaming Benchmark — Push vs Pull**

Panels:
1. **system_lag_ms by system** (line chart, all 3 systems on one axis, per query tab) — primary comparison
2. **kafka_latest_event_time** (single stat, shows producer is live)
3. **producer_lag_ms** (single stat, confirms producer is at wall clock)
4. **probe_duration_seconds** (line chart per system — separates query overhead from staleness)
5. **Q-A finalization lag** (per-minute annotation — when did each closed window appear per system)

---

## Expected Results

| | Flink | ClickHouse | StarRocks |
|---|---|---|---|
| Q-B lag (steady-state) | ~5–50ms | ~500ms–1s | ~1–3s |
| Q-A lag (intra-window) | ~5–50ms | ~500ms–1s | ~1–3s |
| Q-A finalization lag | ~0ms (proc time, closes at boundary) | implicit (stops updating) | implicit |
| Q-C lag | ~5–50ms (OVER, incremental) | ~500ms–1s (re-scan cost grows) | ~1–3s (re-scan cost grows) |
| Q-C cost as data grows | constant | increases (table scan) | increases (table scan) |

The benchmark will show that the push/pull architectural difference, not tuning, determines the freshness floor. Q-C will additionally show diverging probe_duration_ms for ClickHouse and StarRocks as the 5-minute window accumulates more rows.
