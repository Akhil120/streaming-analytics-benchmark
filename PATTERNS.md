# Analytics Pattern Documentation

Six distinct ingestion and query patterns benchmarked in this project. Each pattern represents a different tradeoff across data freshness, query execution speed, and query model (batch vs streaming).

---

## Common Ingestion Backbone

All patterns share the same upstream pipeline:

```
Data Generator
     │  (produces events with embedded event_time timestamp)
     ▼
Apache Kafka  (3-broker KRaft cluster)
     │
     ├──► Pattern 1: ClickHouse Kafka Engine
     │
     ├──► Pattern 6: StarRocks Routine Load (direct Kafka consumer, no Flink)
     │
     └──► Flink Ingestion Job (Kafka Source)
               │
               ▼
           Apache Fluss
               ├── Hot Layer  (Arrow, TabletServer KvStore/LogStore)
               │       ├──► Pattern 2: Flink SQL Union Read (streaming)
               │       └──► Pattern 5: Flink → StarRocks PK Table (streaming write)
               │
               └── Cold Layer  (Parquet via Paimon/Iceberg, tiering service)
                       ├──► Pattern 3: Flink SQL batch read
                       └──► Pattern 4: StarRocks Paimon Catalog (batch OLAP)
```

**Event schema (all patterns use the same source data):**

```sql
CREATE TABLE transactions (
  event_id     STRING,
  user_id      BIGINT,
  amount       DECIMAL(12,2),
  region       STRING,
  event_type   STRING,
  event_time   TIMESTAMP(3),   -- embedded at produce time, used for freshness_lag
  proc_time    AS PROCTIME()
)
```

**Lag metrics captured for every pattern:**

| Metric | Formula | Meaning |
|---|---|---|
| `freshness_lag` | `MAX(NOW() - event_time)` over result set | How stale is the newest data the query can see? |
| `query_latency` | Wall time from submission to last row | How long did the engine take to answer? |
| `result_update_latency` | Time from Kafka produce to result visible | End-to-end pipeline delay (streaming patterns only) |

---

## Pattern 1 — ClickHouse via Kafka Engine

### What it is

ClickHouse's native Kafka integration. A Kafka Engine table acts as a consumer, continuously polling for new messages. A Materialized View transforms and inserts each polled batch into a target MergeTree table. Queries run against the MergeTree table.

### Architecture

```
Kafka Topic
    │
    │  poll (micro-batch)
    ▼
┌──────────────────────────────────────────┐
│  ClickHouse                              │
│                                          │
│  kafka_raw (Kafka Engine table)          │
│       │  Materialized View (continuous)  │
│       ▼                                  │
│  transactions_local (MergeTree)          │
│       │                                  │
│       ▼  SELECT queries                  │
│  Analytical results                      │
└──────────────────────────────────────────┘
```

### How it works

1. `kafka_raw` is a Kafka Engine table — it does not store data itself, it is a consumer interface.
2. The Materialized View fires on every batch polled from Kafka and inserts rows into `transactions_local`.
3. Queries read from `transactions_local`, which is a physical MergeTree table on disk.
4. New events in-flight (not yet polled) are **invisible** to queries. You always query a materialized snapshot.

### Key configuration parameters

| Parameter | Default | Effect |
|---|---|---|
| `kafka_flush_interval_ms` | 7500ms | Max time before flushing an incomplete batch to MergeTree |
| `kafka_poll_timeout_ms` | 500ms | How long each Kafka poll call waits for messages |
| `kafka_consumer_reschedule_ms` | 500ms | Retry delay when no messages available |
| `kafka_num_consumers` | 1 | Parallel consumer threads per table |
| `kafka_poll_max_batch_size` | `max_block_size` | Max messages per poll |

**Tuning for minimum freshness lag:** Set `kafka_flush_interval_ms=500` and `kafka_num_consumers` to match Kafka partition count. Practical minimum: ~500ms–1s freshness under load.

### Query model

- **Batch only.** There is no concept of a continuous/streaming query in ClickHouse.
- Re-running the same query 2 seconds later may return more rows (new data ingested between runs).
- For "all data including incoming new data" you must either re-run the query or use a polling loop.

### Strengths

- Extremely fast OLAP queries — MergeTree columnar storage with vectorized execution
- Simple operational model — no external streaming framework required
- Mature Kafka integration, well-documented
- Excellent for fixed-window aggregations and historical analytics

### Limitations

- **Freshness floor:** Cannot achieve sub-second freshness without significant tuning and resource cost
- **No continuous queries:** Every query is a point-in-time snapshot
- **Upsert semantics are approximate:** ReplacingMergeTree de-duplicates eventually, not immediately — queries may see duplicate rows until background merges complete
- **In-flight data invisible:** Data being polled but not yet committed to MergeTree cannot be queried

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  poll interval + flush interval
                    ▼
Visible in MergeTree  ──► freshness_lag ≈ 500ms – 10s (tunable)
```

---

## Pattern 2 — Flink SQL on Fluss Hot Layer (Union Read, Streaming)

### What it is

The most capable real-time pattern. Flink reads from Apache Fluss using a continuous streaming query. The query transparently combines data from Fluss's **hot layer** (sub-second fresh, Arrow format in TabletServer) and **cold layer** (historical Parquet via Paimon/Iceberg) through Fluss's Union Read feature. A single query sees all historical data plus all incoming new data without re-submission.

### Architecture

```
Kafka Topic
    │
    │  Flink Kafka Source (millisecond polling)
    ▼
Flink Ingestion Job
    │  Fluss Sink (continuous writes)
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Apache Fluss                                               │
│                                                             │
│  ┌─────────────────────────┐  ┌────────────────────────┐   │
│  │  Hot Layer (LogStore +  │  │  Cold Layer            │   │
│  │  KvStore, Arrow format) │  │  (Paimon / Iceberg,    │   │
│  │  TabletServer           │  │  Parquet, compacted)   │   │
│  │  Sub-second freshness   │  │  Minutes freshness     │   │
│  └────────────┬────────────┘  └───────────┬────────────┘   │
│               │    Union Read (Flink only) │               │
│               └────────────┬──────────────┘               │
│                            ▼                               │
│                    Single Logical Table                     │
└─────────────────────────────────────────────────────────────┘
                             │
                             │  Flink SQL continuous query
                             ▼
                    Streaming result updates
                    (new rows emitted as events arrive)
```

### How it works

1. The Flink ingestion job reads from Kafka and writes to a Fluss Primary Key table (for upserts) or Log table (for append-only).
2. Fluss's **TabletServer** stores incoming data in two sub-components:
   - **KvStore**: mutable key-value storage, functions like a database table with change data capture (changelog). Used for Primary Key tables.
   - **LogStore**: append-only log, functions like a database binlog. Serves as WAL for KvStore and as the data source for Log tables.
3. Fluss's **tiering service** asynchronously compacts hot layer data into Paimon/Iceberg format (cold layer). This runs in the background continuously.
4. When a Flink SQL query runs against the Fluss table:
   - **Union Read** combines cold layer (Parquet snapshots from Paimon/Iceberg) with hot layer (live Arrow stream from TabletServer)
   - The result is a unified, ordered, deduplicated view of all data — historical and current
   - For streaming queries, results are emitted continuously as new events commit to the hot layer

### Scan startup modes

Controlled via `scan.startup.mode` (set as SQL hint or table property):

| Mode | Behavior | Use case |
|---|---|---|
| `full` (default) | Full snapshot first, then incremental changes | "All data including new": reads everything from the beginning, stays live |
| `earliest` | Start from earliest changelog offset | Same as `full` but follows changelog semantics for PK tables |
| `latest` | Start from current offset only | Monitoring new events only, no backfill |
| `timestamp` | Start from specific time | Replay from a known point in time |

For your "query all Kafka topic data including incoming new data" requirement: use `full` or `earliest` mode.

### Key optimizations

- **Column pruning**: Arrow format allows reading only queried columns, reducing I/O by up to 80%
- **Partition pruning**: Filter predicates on partition keys eliminate entire partitions, applies dynamically to newly created partitions during streaming

### Query model

- **True streaming.** The query stays open and emits updated results as new events commit to the hot layer.
- No re-submission needed — the query already includes in-flight data within one checkpoint interval.
- For aggregations: results update incrementally as new events arrive.
- For point queries: answered from KvStore (hot) with sub-second latency.

### Strengths

- **Only pattern here with true continuous queries** — the query itself is a live subscription
- Sub-second freshness for incoming data
- Handles both historical backfill and live stream in a single query
- Column and partition pruning reduce I/O significantly
- Native Flink integration — no additional connectors or middleware needed
- Primary Key tables support upserts with exactly-once semantics

### Limitations

- **Union Read is currently Flink-only** — other engines (Spark, StarRocks, Trino) can only read the cold layer
- Requires a running Flink cluster for all queries (not a standalone query engine)
- Streaming query results require a sink (another table, print, or external system) — not interactive ad-hoc queries
- Cold layer freshness for non-Flink engines depends on tiering service compaction frequency

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  Flink source poll (~ms) + checkpoint interval (~1–30s)
                    ▼
Committed to Fluss hot layer  ──► freshness_lag ≈ 100ms – 2s
                    │
                    │  Tiering service compaction (background, async)
                    ▼
Available in cold layer  ──► freshness_lag ≈ minutes (for non-Flink engines)
```

---

## Pattern 3 — Flink SQL Batch Read on Fluss Cold Layer

### What it is

A point-in-time batch query against the Fluss cold layer (Paimon or Iceberg Parquet snapshots). Unlike Pattern 2, this is not continuous — it reads a snapshot of data compacted into the lakehouse format and returns a bounded result set. Used for historical analytical workloads where sub-second freshness is not required.

### Architecture

```
Kafka Topic
    │
    │  (via Pattern 2 ingestion path — same Flink ingestion job)
    ▼
Apache Fluss
    │
    │  Tiering service (async compaction, continuous background process)
    ▼
┌─────────────────────────────────────────────┐
│  Cold Layer: Paimon or Iceberg              │
│  (Parquet files on object storage / HDFS)  │
└─────────────────────────────────────────────┘
    │
    │  Flink batch read (bounded job, scan.startup.mode = full)
    ▼
Bounded result set (query terminates after reading snapshot)
```

### How it works

1. The tiering service compacts Fluss hot layer data into Paimon/Iceberg format on a continuous background basis. Each compaction produces a new snapshot.
2. A Flink batch job (or Flink SQL in batch execution mode) reads the latest Parquet snapshot from the cold layer.
3. The job reads a bounded dataset — it terminates after processing the snapshot, unlike Pattern 2 which stays open.
4. Data written to the hot layer after the snapshot was created is **not included** in this query.

### Distinction from Pattern 2

| Dimension | Pattern 2 (Streaming) | Pattern 3 (Batch) |
|---|---|---|
| Query lifetime | Unbounded — stays open | Bounded — terminates |
| Data coverage | Hot + Cold (Union Read) | Cold only (snapshot) |
| Freshness | Sub-second (hot layer) | Minutes (last compaction) |
| Result delivery | Continuous stream of updates | Single result set |
| Use case | Live dashboards, alerting | Reports, historical aggregations |

### Query model

- **Batch.** Flink batch execution mode, or `SELECT` with `scan.startup.mode = 'full'` in a bounded context.
- Returns all rows up to the latest cold layer snapshot.
- New data arriving after query start is not included.

### Strengths

- Reads optimized Parquet columnar format — efficient for large-scale OLAP
- Bounded execution — predictable resource consumption and termination
- No Flink streaming job required — can be a one-shot batch job
- Works well for scheduled reports, ETL, and historical analysis

### Limitations

- **Freshness bounded by compaction interval** — cannot see data newer than the last tiering service run
- Not suitable for use cases requiring data that arrived in the last few minutes
- Cold layer snapshot may lag hot layer by several minutes depending on compaction frequency and data volume

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  ingestion + tiering/compaction delay
                    ▼
Cold layer snapshot  ──► freshness_lag ≈ 5–30 minutes (compaction-dependent)
```

---

## Pattern 4 — StarRocks via Fluss Paimon/Iceberg Catalog (Batch OLAP)

### What it is

StarRocks reads the Fluss cold layer directly via an external catalog pointing at Paimon or Iceberg. StarRocks does not interact with Fluss's hot layer or the Flink ingestion job — it reads the same compacted Parquet snapshots that Pattern 3 reads, but using StarRocks's own MPP query engine instead of Flink. This is the highest-performance batch OLAP path in this benchmark.

### Architecture

```
Apache Fluss
    │
    │  Tiering service (async compaction)
    ▼
┌───────────────────────────────────────────────────┐
│  Cold Layer: Paimon / Iceberg                     │
│  (Parquet on object storage / local filesystem)   │
└───────────────────────────────────────────────────┘
    │
    │  External Catalog (CREATE EXTERNAL CATALOG)
    ▼
┌──────────────────────────────────────────────────┐
│  StarRocks                                        │
│  FE (query planning, metadata, catalog)           │
│  BE (columnar execution, vectorized engine)       │
│                                                   │
│  SELECT * FROM paimon_catalog.db.transactions    │
│  WHERE ...                                        │
└──────────────────────────────────────────────────┘
```

### How it works

1. StarRocks is configured with a Paimon (or Iceberg) external catalog pointing at the storage location where Fluss's tiering service writes compacted data.
2. When a query runs, StarRocks's FE reads table metadata and snapshot manifests from the catalog.
3. StarRocks's BE nodes execute a vectorized columnar scan of the Parquet files in parallel (MPP).
4. Results are returned to the client. The query reads the latest available snapshot — data not yet compacted by Fluss's tiering service is **invisible**.
5. No Flink cluster is required to run queries — StarRocks is self-contained for this path.

### StarRocks catalog setup

```sql
CREATE EXTERNAL CATALOG paimon_fluss_catalog
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",   -- or "hive" if using HMS
  "paimon.catalog.warehouse" = "s3://your-bucket/fluss-lakehouse/"
);

-- Query directly
SELECT region, SUM(amount), COUNT(*)
FROM paimon_fluss_catalog.analytics.transactions
WHERE event_time >= '2025-01-01'
GROUP BY region;
```

### Query model

- **Batch only.** StarRocks has no concept of a continuous streaming query against Paimon.
- Queries read the latest Parquet snapshot available at query time.
- Re-running the query after a new compaction cycle will return newer data.
- Supports full SQL: aggregations, window functions, JOINs, subqueries.

### Strengths

- **Best raw query performance** for batch OLAP in this benchmark — StarRocks's vectorized MPP engine is purpose-built for this workload
- No Flink dependency for queries — StarRocks handles everything after the catalog is configured
- Decoupled from Fluss's hot layer — StarRocks BE nodes are not competing with streaming ingestion
- Handles very large historical datasets efficiently via Parquet predicate pushdown and column pruning
- Direct SQL access — no streaming framework knowledge required for analysts

### Limitations

- **Cold layer only** — cannot access Fluss hot layer data (Union Read is Flink-only)
- Freshness lag matches tiering service compaction interval (minutes)
- Not suitable for real-time or near-real-time analytical queries
- Read-only access to Paimon tables from StarRocks — cannot write back via this path

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  ingestion + tiering/compaction delay (same as Pattern 3)
                    ▼
Cold layer snapshot available
                    │
                    │  StarRocks catalog metadata refresh + query execution
                    ▼
Query result  ──► freshness_lag ≈ 5–30 minutes (same ceiling as Pattern 3)
              ──► query_latency ≈ milliseconds to seconds (StarRocks MPP, typically faster than Flink batch for OLAP)
```

---

## Pattern 5 — StarRocks via Flink Streaming Write (Near-Real-Time OLAP)

### What it is

Flink reads from the Fluss hot layer (or directly from Kafka) and writes continuously to a StarRocks Primary Key table using the StarRocks Flink connector. This gives StarRocks access to near-real-time data — not sub-second like Pattern 2, but significantly fresher than Pattern 4. StarRocks then serves fast OLAP queries on that data.

### Architecture

```
Apache Fluss (Hot Layer)  ──OR──  Kafka Topic
    │                                   │
    │  Flink Source (streaming read)    │
    └──────────────┬────────────────────┘
                   │
                   ▼
         Flink Streaming Job
         (transformations, aggregations optional)
                   │
                   │  StarRocks Flink Connector (Stream Load)
                   ▼
┌──────────────────────────────────────────────────────────┐
│  StarRocks                                               │
│                                                          │
│  transactions_realtime (Primary Key table)               │
│  ├── Upsert support (merge-on-read for PK)               │
│  ├── Ingestion: micro-batch commits from Flink           │
│  └── Queries: vectorized MPP, same as Pattern 4         │
└──────────────────────────────────────────────────────────┘
```

### How it works

1. A Flink streaming job reads from either Fluss hot layer (via Flink-Fluss connector) or Kafka directly (via Flink Kafka connector).
2. The Flink StarRocks connector batches rows and submits them to StarRocks via **Stream Load** — a HTTP-based bulk insert API.
3. StarRocks commits each Stream Load batch atomically. Committed data is immediately queryable.
4. The StarRocks Primary Key table handles upserts — if a row with the same primary key arrives twice, the newer version replaces the older one.
5. Freshness lag = Flink checkpoint interval + Stream Load commit time (typically 2–10s end-to-end).

### Flink → StarRocks connector configuration

```sql
-- Flink SQL: create StarRocks sink table
CREATE TABLE starrocks_transactions (
  event_id   STRING,
  user_id    BIGINT,
  amount     DECIMAL(12,2),
  region     STRING,
  event_type STRING,
  event_time TIMESTAMP(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector'            = 'starrocks',
  'jdbc-url'             = 'jdbc:mysql://starrocks-fe:9030',
  'load-url'             = 'starrocks-fe:8030',
  'database-name'        = 'analytics',
  'table-name'           = 'transactions_realtime',
  'username'             = 'root',
  'password'             = '',
  'sink.buffer-flush.interval-ms' = '2000',   -- flush every 2s
  'sink.buffer-flush.max-rows'    = '100000'  -- or when 100K rows buffered
);

-- Insert stream
INSERT INTO starrocks_transactions
SELECT * FROM fluss_transactions;
```

### Query model

- **Batch (on live-updating data).** StarRocks itself has no continuous query model — you submit a query and get a point-in-time result.
- However, the underlying table is being continuously updated by Flink, so successive queries see progressively newer data.
- Practically behaves like: "re-query every N seconds to get updated results."
- Supports full SQL: aggregations, window functions, JOINs, materialized views on StarRocks side.

### Distinction from Pattern 4

| Dimension | Pattern 4 (Catalog) | Pattern 5 (Flink Stream Write) |
|---|---|---|
| Data freshness | Minutes (cold compaction) | Seconds (Flink checkpoint) |
| StarRocks table type | External (read-only Paimon) | Native PK table (read-write) |
| Flink dependency | None (for queries) | Required (for ingestion) |
| Upsert support | Paimon handles it | StarRocks PK table handles it |
| Query performance | Same MPP engine | Same MPP engine |
| Use case | Historical analytics | Near-real-time dashboards |

### Strengths

- Near-real-time freshness (seconds) with StarRocks's full OLAP query power
- StarRocks native PK table — efficient upsert handling, no read amplification on deduplication
- No cold layer dependency — data path is shorter: Kafka/Fluss → Flink → StarRocks
- Best of both worlds: Flink handles streaming complexity, StarRocks handles analytical query performance
- Scales independently — Flink ingestion throughput and StarRocks query throughput scale separately

### Limitations

- **Requires a running Flink job** at all times for ingestion — operational overhead
- Freshness is bounded by Flink checkpoint interval (cannot be sub-second without significant Flink tuning)
- Two systems to maintain (Flink + StarRocks) vs Pattern 4 (StarRocks only)
- StarRocks still has no continuous query model — analysts must re-run queries to see new data

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  Flink Kafka/Fluss source poll + checkpoint interval
                    ▼
Flink checkpoint committed
                    │
                    │  Stream Load HTTP commit to StarRocks
                    ▼
Visible in StarRocks  ──► freshness_lag ≈ 2–10s (Flink checkpoint + commit)
                      ──► query_latency ≈ ms to seconds (StarRocks MPP)
```

---

## Pattern 6 — StarRocks via Routine Load (Direct Kafka Consumer)

### What it is

StarRocks's built-in persistent Kafka consumer, called **Routine Load**. No Flink or external connector required — StarRocks's FE manages the consumer job lifecycle and its BE nodes execute ingestion directly from Kafka partitions. This pattern occupies the same freshness class as Pattern 1 (ClickHouse Kafka Engine) but substitutes StarRocks's MPP query engine for ClickHouse's MergeTree. It is the simplest path to near-real-time OLAP on StarRocks.

### Architecture

```
Kafka Topic
    │
    │  Routine Load (StarRocks FE manages job, BE consumes partitions)
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  StarRocks                                                       │
│                                                                  │
│  FE  ──► splits job into load tasks per Kafka partition         │
│          schedules tasks to available BE nodes                   │
│                                                                  │
│  BE  ──► each BE consumes assigned partitions                   │
│          parses / filters messages                               │
│          distributes rows to executor BEs for disk write         │
│                                                                  │
│  transactions_routine (Duplicate / Unique / Primary Key table)  │
│       │                                                          │
│       ▼  SELECT queries                                          │
│  Analytical results                                              │
└─────────────────────────────────────────────────────────────────┘
```

### How it works

1. A `CREATE ROUTINE LOAD` statement defines the job: Kafka brokers, topic, consumer group, format, target table, and batching policy.
2. The FE splits the job into **load tasks** — one or more tasks per Kafka partition — and schedules them to BE nodes.
3. Each load task runs until it hits either a **message count limit** (`max_routine_load_batch_size`) or a **time limit** (`routine_load_task_consume_second`), then commits and a new task starts.
4. The `max_batch_interval` property controls the scheduling cadence between consecutive tasks, setting the floor for freshness lag.
5. Committed data is immediately queryable in the target StarRocks table.
6. The FE continuously monitors job health. If error rows exceed the configured threshold, the job auto-pauses; it can be manually resumed with `RESUME ROUTINE LOAD`.

### Routine Load setup

```sql
-- Target table (Primary Key for upserts, Duplicate Key for append)
CREATE TABLE transactions_routine (
  event_id     VARCHAR(64)    NOT NULL,
  user_id      BIGINT,
  amount       DECIMAL(12,2),
  region       VARCHAR(32),
  event_type   VARCHAR(32),
  event_time   DATETIME
) PRIMARY KEY (event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Create the Routine Load job
CREATE ROUTINE LOAD analytics.transactions_load ON transactions_routine
COLUMNS TERMINATED BY ",",
COLUMNS (event_id, user_id, amount, region, event_type, event_time)
PROPERTIES (
  "desired_concurrent_number" = "4",         -- parallel load tasks
  "max_batch_interval"        = "5",         -- flush at least every 5s
  "max_batch_rows"            = "300000",    -- or when 300K rows consumed
  "max_batch_size"            = "209715200", -- or when 200MB consumed
  "format"                    = "json",
  "jsonpaths"                 = '["$.event_id","$.user_id","$.amount","$.region","$.event_type","$.event_time"]',
  "strict_mode"               = "false"
)
FROM KAFKA (
  "kafka_broker_list"  = "kafka-1:9092,kafka-2:9092,kafka-3:9092",
  "kafka_topic"        = "transactions",
  "property.group.id"  = "starrocks-routine-load"
);

-- Monitor job state
SHOW ROUTINE LOAD FOR analytics.transactions_load\G
```

### Key configuration parameters

| Parameter | Default | Effect on freshness |
|---|---|---|
| `max_batch_interval` | 5s | Max seconds between consecutive task runs — directly sets freshness floor |
| `max_batch_rows` | 200,000 | Triggers a commit when this many rows are buffered — higher = more latency but fewer commits |
| `max_batch_size` | 100MB | Triggers a commit at this byte threshold |
| `desired_concurrent_number` | 3 | Task parallelism — match to Kafka partition count for maximum throughput |
| `routine_load_task_consume_second` | 15s | Time budget per task execution |

**Tuning for minimum freshness lag:** Set `max_batch_interval=1` and `desired_concurrent_number` = number of Kafka partitions. Practical minimum: ~1–3s freshness under load.

### Supported formats

| Format | Notes |
|---|---|
| `json` | One JSON object per Kafka message; `jsonpaths` for field extraction |
| `csv` | Configurable delimiter (up to 50 bytes); nulls as `\N` |
| `avro` | Requires Schema Registry; `confluent.schema.registry.url` property |

### Comparison to Pattern 1 (ClickHouse Kafka Engine)

Both patterns are direct Kafka consumers with micro-batch ingestion into a columnar OLAP engine. The key differences:

| Dimension | P1 ClickHouse | P6 StarRocks Routine Load |
|---|---|---|
| Ingestion mechanism | Kafka Engine + Materialized View | Routine Load (FE-managed job) |
| Freshness floor | ~500ms (tunable) | ~1s (tunable) |
| Upsert semantics | Eventual (ReplacingMergeTree) | Exact (Primary Key table, immediate) |
| Error recovery | Manual (re-create consumer) | Automatic (auto-pause + resume) |
| Transformation | Materialized View SQL | COLUMNS clause + column expressions |
| Monitoring | ClickHouse system tables | `SHOW ROUTINE LOAD` |
| Query engine | ClickHouse vectorized MergeTree | StarRocks MPP vectorized |
| Exactly-once delivery | No | Yes |

### Comparison to Pattern 5 (StarRocks via Flink)

| Dimension | P5 Flink → StarRocks | P6 StarRocks Routine Load |
|---|---|---|
| Freshness | 2–10s | 1–5s (roughly comparable) |
| Flink dependency | Yes — required at all times | No — self-contained in StarRocks |
| Transformation capability | Full Flink SQL (joins, enrichment) | Column expressions only (no joins) |
| Exactly-once | Yes (Flink + Stream Load) | Yes (Routine Load) |
| Operational complexity | High | Low |
| Use case | Complex streaming transformations | Simple field mapping, raw ingestion |

### Query model

- **Batch only.** Same as Pattern 1 — queries are point-in-time snapshots against the target table.
- No continuous query capability.
- Re-running the query returns newer data as Routine Load commits new batches.

### Strengths

- **Zero external dependencies** — no Flink, no connectors outside StarRocks itself
- Exactly-once delivery semantics — data is neither lost nor duplicated
- Automatic failure recovery — FE auto-pauses on error threshold, operator resumes
- Supports all StarRocks table types including Primary Key (true upsert, immediate consistency)
- Avro + Schema Registry support out of the box
- Scales horizontally — adding BE nodes increases ingestion parallelism automatically

### Limitations

- **No join/enrichment capability during ingestion** — simple column mapping only; complex transformations require Flink (Pattern 5)
- Freshness floor slightly higher than ClickHouse due to FE task scheduling overhead
- Job management overhead — each topic/table pair needs its own Routine Load job
- No streaming query model — queries are always point-in-time

### Freshness lag profile

```
Produce to Kafka  ──┐
                    │  FE schedules task → BE consumes → batch limit or max_batch_interval
                    ▼
Committed to StarRocks PK table  ──► freshness_lag ≈ 1–5s (tunable via max_batch_interval)
                                 ──► query_latency ≈ ms to seconds (StarRocks MPP)
```

---

## Pattern Comparison Summary

| | Pattern 1 | Pattern 2 | Pattern 3 | Pattern 4 | Pattern 5 | Pattern 6 |
|---|---|---|---|---|---|---|
| **Engine** | ClickHouse | Flink + Fluss | Flink + Fluss | StarRocks | StarRocks + Flink | StarRocks |
| **Data layer** | ClickHouse MergeTree | Fluss Hot (Union) | Fluss Cold | Fluss Cold (Paimon) | StarRocks PK native | StarRocks PK native |
| **Freshness lag** | 1–10s | ~100ms–2s | 5–30 min | 5–30 min | 2–10s | 1–5s |
| **Query model** | Batch (re-run) | Continuous streaming | Batch (bounded) | Batch (re-run) | Batch (re-run, live) | Batch (re-run, live) |
| **Sees in-flight data** | No | Yes (hot layer) | No | No | No | No |
| **True streaming query** | No | Yes | No | No | No | No |
| **OLAP query speed** | Excellent | Moderate | Moderate | Excellent | Excellent | Excellent |
| **Upsert semantics** | Eventual (ReplacingMT) | Exact (KvStore) | Exact (Paimon) | Exact (Paimon) | Exact (PK table) | Exact (PK table) |
| **Exactly-once delivery** | No | Yes | Yes | Yes | Yes | Yes |
| **Operational complexity** | Low | Medium | Medium | Low | High | Low |
| **Flink required** | No | Yes | Yes | No | Yes (ingestion) | No |
| **Kafka dependency at query time** | No | No | No | No | No | No |

---

## Freshness vs Query Speed Tradeoff (Visual)

```
Query Speed
(faster ↑)

Excellent  │  P1 ClickHouse    P6 StarRocks/RL   P5 StarRocks/Flink   P4 StarRocks/Paimon
           │    (1–10s)           (1–5s)              (2–10s)              (5–30m)
           │
Moderate   │                                  P3 Flink Batch
           │                                     (5–30m fresh)
           │
           │        P2 Flink/Fluss Union Read
           │           (~100ms–2s fresh, continuous)
           │
           └──────────────────────────────────────────────────────────► Freshness
                 Sub-second        Seconds              Minutes
```

Note: P2 occupies a unique position — it is the only pattern with true continuous queries, making "query speed" a different dimension entirely (results flow rather than terminate). P6 and P5 occupy similar freshness territory but P6 requires no Flink.

---

## Benchmark Workloads

All six patterns will run the same three query types to enable direct comparison:

### Q1 — Aggregate (batch)
```sql
SELECT region, COUNT(*), SUM(amount), AVG(amount)
FROM transactions
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY region
ORDER BY SUM(amount) DESC;
```

### Q2 — Windowed (time-based)
```sql
SELECT
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  region,
  COUNT(*) AS event_count,
  SUM(amount) AS total_amount
FROM transactions
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), region;
```

### Q3 — Freshness probe
```sql
SELECT
  MAX(event_time)                          AS newest_event_time,
  NOW()                                    AS query_time,
  DATEDIFF('second', MAX(event_time), NOW()) AS freshness_lag_seconds,
  COUNT(*)                                 AS total_rows
FROM transactions;
```

Q3 is the core freshness measurement query — run it continuously (every 5 seconds) across all patterns to produce a freshness lag time series.
