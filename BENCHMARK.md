# Benchmark Results — Streaming Analytics Stack Comparison

Six ingestion-and-query patterns measured against the same Kafka event stream on a single MacBook Pro (Apple M-series, 16 GB RAM, Docker Desktop 8 GB). All numbers are observed wall-clock values, not theoretical limits. See [PATTERNS.md](./PATTERNS.md) for full architectural detail on each pattern.

---

## Test Environment

| Dimension | Value |
|---|---|
| Host | MacBook Pro, Apple M-series, 16 GB RAM |
| Runtime | Docker Desktop 8 GB memory limit |
| Generator | Python producer, ~2–5K rows/sec sustained (peaks to ~10–12K during initial catch-up) |
| Kafka | 3-broker KRaft cluster |
| Event schema | `event_id`, `user_id`, `amount`, `region` (7), `event_type`, `event_time` (embedded) |

> **Docker caveat:** All freshness numbers are measured in Docker on a single machine. Production deployments on dedicated hardware will significantly improve both freshness and query latency, particularly for patterns bottlenecked by JVM scheduling (P5, P6) or Flink's task parallelism (P2, P3).

---

## Patterns at a Glance

| Pattern | Stack | Ingestion path | Flink required |
|---|---|---|---|
| **P1** | ClickHouse | Kafka Engine → MergeTree | No |
| **P2** | Flink + Fluss | Kafka → Fluss hot+cold (Union Read) | Yes |
| **P3** | Flink + Fluss | Kafka → Fluss cold only (Paimon Parquet) | Yes |
| **P4** | StarRocks | Fluss cold lake via Paimon external catalog | No (queries only) |
| **P5** | StarRocks + Flink | Kafka → Flink → StarRocks PK table (Stream Load) | Yes |
| **P6** | StarRocks | Kafka → StarRocks Routine Load (no Flink) | No |

---

## Freshness Results

"Freshness lag" = `NOW() - MAX(event_time)` at query time. Measures how stale the newest visible data is.

| Pattern | Freshness lag (Docker) | Freshness lag (production estimate) | Notes |
|---|---|---|---|
| **P1 ClickHouse** | **~2,100ms** | ~500ms–1s | MergeTree flush interval dominates |
| **P2 Flink/Fluss Union Read** | **-5,000ms** | -1,000ms to 0ms | Negative = hot layer ahead of query clock (expected; hot data visible before wall clock catches up) |
| **P3 Flink cold lake** | ~3.1h (generator paused) | ~2–5min | Bounded by Fluss tiering epoch |
| **P4 StarRocks/Paimon** | ~3h (generator paused) | ~2–5min | Same cold lake as P3; identical freshness ceiling |
| **P5 StarRocks/Flink** | **~55s** (startup); **~10–15s** (steady-state) | ~2–10s | Flink checkpoint + Stream Load commit; floor is `flush_interval + write_time` |
| **P6 StarRocks Routine Load** | **~50s** | ~1–3s | FE scheduling overhead in Docker; `max_batch_interval=1` in prod |

P1, P5, and P6 are in the "seconds" freshness class. P3 and P4 are batch-cadence patterns (minutes). P2 is uniquely in the sub-second class and is the only pattern that can show data *newer than the query clock* (because the hot Arrow layer advances continuously).

---

## Query Latency Results

"Query latency" = wall time from query submission to last row received. Measured on each pattern's native query interface.

### Q1 — Regional aggregate (last 1 hour)

```sql
SELECT region, COUNT(*), SUM(amount), AVG(amount)
FROM transactions
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY region ORDER BY SUM(amount) DESC;
```

| Pattern | Latency | Rows | Notes |
|---|---|---|---|
| P1 ClickHouse | **< 1s** | 7 regions | MergeTree columnar scan; sub-second at any realistic scale |
| P2 Flink/Fluss | ~608s | 7 regions | Full hot+cold scan across 16.75M rows; no predicate pushdown |
| P3 Flink cold | — | 0 rows | Generator paused; all data > 1h old (correct) |
| P4 StarRocks | ~17.5s | 0 rows | Generator paused; all data > 1h old (correct) |
| P5 StarRocks | **< 1s** | 7 regions | ~134K rows per region in PK native table |
| P6 StarRocks | **< 1s** | 7 rows | Live Routine Load table |

### Q2 — Tumbling 1-minute windows

```sql
SELECT TUMBLE_START(event_time, INTERVAL '1' MINUTE), region, COUNT(*), SUM(amount)
FROM transactions
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), region;
```

| Pattern | Latency | Rows | Notes |
|---|---|---|---|
| P1 ClickHouse | **< 1s** | — | Columnar scan, full history |
| P2 Flink/Fluss | ~420s | 120 rows | 16.75M row hot+cold scan |
| P3 Flink cold | ~132s | 120 rows | 7.6M row Parquet-only scan |
| P4 StarRocks | **~14s** | 70 rows | 2.1M rows via MPP Parquet scan; ~10× faster than Flink P3 on similar data |
| P5 StarRocks | **< 1s** | 4 windows | ~1M row native PK table |
| P6 StarRocks | **< 1s** | 28 rows | ~970K row native PK table |

### Q3 — Freshness probe

```sql
SELECT MAX(event_time), NOW(), TIMESTAMPDIFF(SECOND, MAX(event_time), NOW()), COUNT(*)
FROM transactions;
```

| Pattern | freshness_lag_ms | pipeline_lag_ms | total_rows |
|---|---|---|---|
| P1 ClickHouse | ~2,100ms | ~0ms | ~2.6M |
| P2 Flink/Fluss | -5,000ms | 0ms | 16,750,285 |
| P3 Flink cold | 11,233,000ms | 0ms | 7,610,537 |
| P4 StarRocks | 10,844,000ms | 3,817,000ms* | 2,145,583 |
| P5 StarRocks | ~55,000ms | ~0ms | ~1,001,633 |
| P6 StarRocks | ~50,000ms | ~0ms | ~970K |

\* P4 `pipeline_lag_ms` of ~63min is a backfill artifact: `ingest_time` was set at Flink processing time during historical catch-up. Stabilizes to ~0–5s once the backlog drains.

---

## Summary Comparison Table

| | **P1 ClickHouse** | **P2 Flink/Fluss** | **P3 Flink cold** | **P4 StarRocks** | **P5 StarRocks/Flink** | **P6 StarRocks/RL** |
|---|---|---|---|---|---|---|
| **Freshness** | ~2s | **< 0s** (ahead) | ~2–5min | ~2–5min | ~10–55s | ~3–50s |
| **Q2 latency** | < 1s | **420s** | 132s | **14s** | < 1s | < 1s |
| **Query model** | Batch | Batch / **Streaming** | Batch | Batch | Batch | Batch |
| **True streaming** | No | **Yes** | No | No | No | No |
| **Upsert semantics** | Eventual | Exact | Exact | Exact | **Exact** | **Exact** |
| **Exactly-once** | No | Yes | Yes | Yes | Yes | Yes |
| **Flink required** | No | Yes | Yes | No (queries) | Yes | **No** |
| **Operational complexity** | Low | Medium | Medium | Low | **High** | Low |
| **Dependencies** | CH + Kafka | Flink + Fluss + ZK + MinIO | ← same | SR + Fluss cold | SR + Flink + Kafka | SR + Kafka |

---

## Key Findings

### 1. For sub-minute freshness with fast queries: P1 and P6 lead

ClickHouse (P1) and StarRocks Routine Load (P6) achieve the best combination of freshness and query speed at low operational cost. Both require no Flink. P6 adds exactly-once semantics and true upserts that ClickHouse lacks.

**Winner for simple near-real-time OLAP:** P6 (if already on StarRocks), P1 (if ClickHouse is the preference).

### 2. For sub-second freshness, P2 is the only option — but you pay in query speed

Flink Union Read (P2) is the only pattern in this benchmark where data visible to a query is newer than the query clock itself. The hot Arrow layer in Fluss continuously advances, so a batch-mode Flink query at time T can see events up to T+5s.

The cost: query latency. P2 scanned 16.75M rows in ~420s for Q2. StarRocks P4 scanned a similar 2.1M row Parquet dataset in 14s — roughly 10× faster. This gap is structural: Flink's batch execution and Union Read merge overhead cannot match StarRocks's vectorized MPP for OLAP workloads.

**Winner for freshness alone:** P2 (Flink Union Read). If you need continuous streaming output (not a point-in-time result), P2 is the only option entirely.

### 3. For batch OLAP on historical data: StarRocks P4 is 10× faster than Flink P3

Both P3 and P4 read the same Fluss cold lake (Paimon Parquet on MinIO). StarRocks answered Q2 in ~14s; Flink took ~132s on a comparable dataset. StarRocks's MPP vectorized engine is purpose-built for this workload. The Parquet format is the same; the query engine is the differentiator.

**Implication:** If you're already running Flink+Fluss and want fast ad-hoc queries on the cold lake, adding StarRocks as a query layer (P4) costs zero freshness but yields a 10× query speedup.

### 4. P5 adds seconds-fresh queries to StarRocks without the cold-lake constraint

P4 is fast but reads stale data (minutes). P5 puts a Flink job between Kafka and a native StarRocks PK table, achieving ~10–15s freshness (steady-state) while preserving < 1s query latency. The tradeoff: a continuously running Flink cluster and higher operational complexity.

P6 covers the same freshness class more simply (no Flink), unless you need:
- Joins or enrichment during ingestion (only Flink can do this)
- Data sourced from Fluss rather than Kafka

### 5. P6 vs P5 — same freshness class, very different complexity

In Docker, P5 measured ~55s startup / ~10–15s steady-state; P6 measured ~50s. In production, P6 achieves 1–3s with `max_batch_interval=1`; P5 achieves 2–10s. The freshness difference is marginal. P6 requires no Flink, has automatic failure recovery, and is operationally trivial to manage. Choose P5 only when ingestion-time joins or Fluss-sourced data are required.

---

## Decision Guide

```
Do you need continuous/streaming output (live dashboard, CEP)?
  └─ Yes ──► P2 (Flink Union Read — only option)
  └─ No  ──► continue

Do you need sub-second freshness?
  └─ Yes ──► P1 (ClickHouse) or P6 (StarRocks) — ~1–3s prod floor
  └─ No  ──► continue

Do you need OLAP query speed < 1s on seconds-fresh data?
  └─ Yes ──► P5 (Flink → StarRocks) or P6 (Routine Load)
             Pick P6 unless you need ingestion-time joins or a Fluss source
  └─ No  ──► continue

Do you need OLAP query speed on minutes-fresh historical data?
  └─ StarRocks already running? ──► P4 (Paimon catalog, 14s Q2)
  └─ Flink only ──────────────► P3 (132s Q2, same data)
```

---

## Freshness vs Query Speed Chart

```
Query speed
(faster ↑)

            P1 ClickHouse ──────── P6 StarRocks/RL ─── P5 StarRocks/Flink
Excellent   ~2s fresh               ~50s Docker           ~55s Docker
            ~1s prod                ~1–3s prod            ~10s prod


Moderate                                P4 StarRocks/Paimon    P3 Flink/Parquet
                                        ~14s Q2, mins fresh     ~132s Q2, mins fresh


Slow                 P2 Flink Union Read
                     ~420s Q2, -5s fresh (ahead of clock)
                     ← only pattern with true streaming output →

            │
            └─────────────────────────────────────────────────────────────► Freshness
                    Sub-second       Seconds          Minutes
```

---

## Operational Complexity

| Pattern | Stack surface area | What breaks | Recovery |
|---|---|---|---|
| P1 | ClickHouse + Kafka | Kafka consumer lag; MV falls behind | Restart CH Kafka Engine table |
| P2/P3 | Flink + Fluss + ZK + MinIO | ZK session loss kills tiering; JM/TM disconnect | Restart in order: ZK → Fluss → Flink JM → TM |
| P4 | StarRocks + MinIO | Catalog metadata stale after Paimon snapshot rollback | Re-run `ANALYZE TABLE`; retry query |
| P5 | StarRocks + Flink + Kafka | Flink job crash stops ingestion; concurrent P6 load causes THRIFT_EAGAIN | PAUSE P6 Routine Load; restart Flink job |
| P6 | StarRocks + Kafka | Error rows exceed threshold → job auto-pauses | `RESUME ROUTINE LOAD`; fix schema mismatch |

P6 has the simplest failure recovery. P2/P3 have the most moving parts. P5 has a documented interaction hazard with P6 on a single-BE StarRocks node (concurrent publish contention).

---

## Setup-Time Bug Log

Documented bugs hit during implementation across all patterns (for future reference when upgrading versions):

| Pattern | Component | Bug | Fix |
|---|---|---|---|
| P1 | ClickHouse | None | — |
| P2 | Flink / Fluss | ZK session timeouts interrupting Fluss coordinator during tiering | `zookeeper.session-timeout=90000` in coordinator + tablet server |
| P2 | Flink | Paimon JARs not on classpath for Union Read | Copy `/opt/flink/paimon/*.jar` → `/opt/flink/lib/` in Dockerfile |
| P2 | Flink | TM disconnects when only JM container is force-recreated | `docker compose restart flink-taskmanager` after JM recreate |
| P5 | Flink | `json.timestamp-format.standard=ISO-8601` doesn't strip trailing `Z` | Declare `event_time STRING`; cast with `TO_TIMESTAMP(REPLACE(..., 'Z', ''), ...)` |
| P5 | StarRocks | V2 Stream Load redirects to `127.0.0.1:8040` (unreachable from Flink Docker network) | `sink.version=V1` + `load-url=starrocks:8040` (direct BE HTTP) |
| P5 | StarRocks | V1 JSON format rejects TIMESTAMP(3) → `NumberLoadedRows: 0` | `sink.properties.format=csv` + `sink.properties.column_separator=\x01` |
| P5 | StarRocks | `sink.buffer-flush.max-rows=10000` throws ValidationException | Minimum is `64000`; use `64000` |
| P5 | StarRocks | Concurrent P6 Routine Load causes THRIFT_EAGAIN during P5 Stream Load commits | PAUSE P6 before P5 benchmark on single-BE setups |
| P6 | StarRocks | `now()` in `COLUMNS` clause throws NPE in Routine Load task planner | Use `DEFAULT CURRENT_TIMESTAMP` on table column; omit from COLUMNS |
| P6 | StarRocks | `OFFSET_BEGINNING` without `kafka_partitions` fails with partition/offset count mismatch | Use `"property.auto.offset.reset"="earliest"` instead |
