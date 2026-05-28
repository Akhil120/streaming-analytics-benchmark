# Flink + Fluss Lakehouse — Deep Dive

This document covers the full architecture, configuration, and operational details for Patterns 2 and 3 in this benchmark: streaming ingestion into Apache Fluss and the Paimon lakehouse cold tier.

This setup is significantly more complex than ClickHouse or StarRocks Routine Load. Read this before making configuration changes.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Kafka (broker:29092)                                                    │
│  topic: transactions                                                     │
└────────────────────────┬─────────────────────────────────────────────────┘
                         │  Flink Kafka Source
                         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Flink Streaming Ingestion Job   (01_kafka_to_fluss.sql, parallelism=1) │
│  • Parses JSON                                                           │
│  • Casts event_time string → TIMESTAMP(3)                               │
│  • Stamps ingest_time = CURRENT_TIMESTAMP                               │
└────────────────────────┬─────────────────────────────────────────────────┘
                         │  Fluss Sink (Fluss catalog, PK table, upsert)
                         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Apache Fluss (coordinator-server + tablet-server)                      │
│                                                                          │
│  analytics.transactions  (Primary Key table, event_id)                  │
│                                                                          │
│  ┌──────────────────────────┐      ┌──────────────────────────────────┐ │
│  │  Hot Layer               │      │  Cold Layer (Paimon)             │ │
│  │  KvStore + LogStore      │      │  Parquet snapshots               │ │
│  │  Arrow format, RAM       │      │  /tmp/paimon/warehouse/          │ │
│  │  Sub-second freshness    │      │  Tiered every 30m                │ │
│  └──────────────────────────┘      └──────────────────────────────────┘ │
│                │                                  │                      │
│                └──────────Union Read──────────────┘                      │
└──────────────────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
  P2: query `transactions`           P3: query `transactions$lake`
  (Union Read: hot + cold)           (cold Parquet scan only)
  freshness: ~500ms – 3s             freshness: ~30min+
```

### Tiering service (separate Flink job)

The tiering service is a standalone Flink streaming job that reads Fluss KV snapshots and writes them into the Paimon lake format on the local filesystem. It runs continuously alongside the ingestion job.

```
Fluss tablet-server (KvStore snapshot)
         │
         │  TieringSource (reads Fluss log offsets)
         ▼
 Flink Tiering Job  (fluss-flink-tiering-0.9.1-incubating.jar)
         │  PaimonLakeWriter (writes Parquet)
         │  TieringCommitter (commits Paimon snapshot at each checkpoint)
         ▼
 /tmp/paimon/warehouse/analytics.db/transactions/
   ├── schema/schema-0
   ├── snapshot/snapshot-N        ← appears only at checkpoint boundaries
   ├── manifest/...
   └── bucket-0/data-*.parquet
```

**Critical:** Paimon snapshots only appear at Flink checkpoint boundaries. If checkpointing is not configured, Parquet files will be written but `snapshot/` will never appear, so P3 queries will fail with "no snapshot found."

---

## Container Roles

| Container | Image | Role |
|---|---|---|
| `zookeeper` | `zookeeper:3.9.2` | Fluss cluster metadata + leader election |
| `minio` | `minio/minio` | S3-compatible object store for Fluss remote log segments **and** Paimon warehouse |
| `minio-init` | `minio/mc` | One-shot: creates `fluss` bucket |
| `coordinator-server` | `apache/fluss:0.9.1-incubating` | Fluss cluster coordinator — manages tablet assignment, lake snapshot commits |
| `tablet-server` | `apache/fluss:0.9.1-incubating` | Stores hot data (Arrow/KvStore/LogStore), handles Fluss reads/writes |
| `flink-jobmanager` | (local build) | Flink JM + SQL Gateway (port 8083) |
| `flink-taskmanager` | (local build) | Flink TM — runs both the ingestion job and tiering job |

### Why the Paimon warehouse is on MinIO, not a local volume

An earlier design used a `paimon-warehouse` Docker named volume with a `paimon-init` busybox container to `chown -R 9999 /tmp/paimon/warehouse` so the Flink JVM (uid 9999) could write. This worked once but broke after every `make clean`: `restart: "no"` containers are not restarted by subsequent `docker compose up`, so the volume was recreated as root-owned and every tiering epoch failed immediately with `Mkdirs failed to create .../bucket-0`.

Using `s3://fluss/paimon-warehouse` on MinIO eliminates the ownership problem entirely — MinIO has no POSIX permissions, and the bucket survives `make clean` cycles if you only recreate volumes rather than the bucket.

---

## Flink Image

Built from `docker/flink/Dockerfile`. Key additions on top of `apache/fluss-quickstart-flink:1.20-0.9.1-incubating`:

- `flink-sql-connector-kafka-3.4.0-1.20.jar` — Kafka source for the ingestion job
- `paimon-flink-1.20-0.9.0.jar` — Paimon table format for the tiering writer
- `hadoop-client-api-3.3.6.jar` + `hadoop-hdfs-client-3.3.6.jar` — Hadoop FileSystem API used by Paimon for S3 warehouse access

---

## Critical Configuration

### `classloader.check-leaked-classloader: false`

Set on both JM and TM. Without this, the tiering job fails with:

```
IllegalStateException: Trying to access closed classloader. This can happen when code
(such as serializers) accesses the environment's classloader after a task was finished.
```

Root cause: Paimon uses Hadoop's `Configuration`, which lazily loads XML resources. By the time Paimon reads Parquet column stats during the `TieringCommitter.commit()` call, Flink's safety net wrapper has already closed the job classloader. Setting this flag suppresses the safety net check, which is safe here since the access is benign.

### `table.datalake.freshness: 30m`

Set on the Fluss `transactions` table (in `01_kafka_to_fluss.sql`). This property controls **both** the tiering epoch interval and the per-epoch max duration. Setting it too low causes every tiering epoch to fail: the Fluss KV snapshot download alone takes 40–70 seconds (grows with data volume), and if `table.datalake.freshness` is shorter than that, the tiering service fires a `TieringReachMaxDurationEvent` during the download phase, the epoch is abandoned without writing any Parquet files, and `snapshot/` never appears in the warehouse.

**Why not `30s`** (the Fluss quickstart default): after even 30 minutes of generator traffic at ~3K msgs/sec, the accumulated KV snapshot is large enough that the download alone exceeds 30 seconds, causing every epoch to fail before any Parquet files are written. With `30m`, the download (35–70s) and Paimon sort+write (10–20 min for ~20M rows) both fit within the window.

Cold-lake benchmark `freshness_lag_ms` will reflect the 30-minute epoch cadence. This is a consequence of data volume, not a fundamental Fluss limitation — a smaller or partitioned table would tier far faster.

### `taskmanager.memory.process.size: 3500m`

Flink standalone mode does not read Docker container memory limits. Without an explicit size, the TM defaults to ~512MB heap, which causes `OutOfMemoryError` during the first large compaction pass. The TM container has a 4GB Docker memory limit; `3500m` gives the JVM ~1.3GB heap.

### S3 / MinIO delegation token

Fluss uses a delegation token mechanism for S3 credentials. The JobManager (`FlussS3DelegationTokenProvider`) calls AWS STS `AssumeRole` and distributes short-lived credentials to TaskManagers. For MinIO (which implements the STS API), you must configure both:

```yaml
s3.assumed.role.arn: arn:aws:iam::000000000000:role/minioadmin
s3.assumed.role.sts.endpoint: http://minio:9000
```

These must appear in:
1. Fluss `coordinator-server` and `tablet-server` `FLUSS_PROPERTIES`
2. Flink `flink-jobmanager` and `flink-taskmanager` `FLINK_PROPERTIES`

Without `s3.assumed.role.sts.endpoint`, the provider calls `https://sts.amazonaws.com` from inside Docker, fails, and the TM crashes with `NoAwsCredentialsException`.

### Checkpointing on the tiering job

The tiering JAR must be submitted with checkpointing enabled:

```json
"flinkConfiguration": {
  "execution.checkpointing.interval": "60000",
  "execution.checkpointing.mode": "AT_LEAST_ONCE"
}
```

This is part of the `make flink-submit-tiering` REST API call body. Without it, the Paimon `TieringCommitter` never calls `commit()` (it is a `CheckpointedFunction` that only commits inside `notifyCheckpointComplete`), so `snapshot/` never appears in the warehouse.

---

## Job Submission

Both jobs must be submitted manually after `make up` (there is no auto-init container):

```bash
make flink-submit
```

This runs two targets in sequence:

1. **`flink-submit-tiering`** — uploads `fluss-flink-tiering-0.9.1-incubating.jar` to the Flink REST API (`/jars/upload`) and submits it with checkpointing configured via the REST body. The JAR lives at `/opt/flink/opt/` inside the JM container.

2. **`flink-submit-sql`** — pipes `docker/flink/sql/01_kafka_to_fluss.sql` to `sql-client.sh` connected to the SQL Gateway. The SQL creates the Fluss catalog, `analytics` database, `transactions` table (with `table.datalake.enabled=true`), the ephemeral Kafka source, and submits the streaming INSERT.

> **Note on `sql-client.sh -f` vs stdin pipe:** In SQL Gateway mode (`-e http://...`), the `-f <file>` flag silently skips execution. Always pipe via stdin: `cat file.sql | sql-client.sh -e http://...`.

After submission, verify both jobs are RUNNING:

```bash
make flink-jobs
# Expected: two jobs with "status": "RUNNING"
```

---

## Verifying Each Layer

### Hot layer (Fluss)

```bash
make flink-sql
# In SQL client:
USE CATALOG fluss_catalog;
USE analytics;
SET 'execution.runtime-mode' = 'BATCH';
SELECT COUNT(*) FROM transactions;
# Should return a non-zero count within ~10s of job start
```

### Cold layer — Parquet files

```bash
docker compose exec minio mc ls local/fluss/paimon-warehouse/analytics.db/transactions/bucket-0/ 2>/dev/null | head
# Should show data-*.parquet and changelog-*.parquet files
```

Or via the MinIO console at http://localhost:9001 → browse `fluss/paimon-warehouse/analytics.db/transactions/`.

### Cold layer — Paimon snapshot (required for P3)

```bash
docker compose exec minio mc ls local/fluss/paimon-warehouse/analytics.db/transactions/snapshot/ 2>/dev/null
# Expected: LATEST  snapshot-1  snapshot-2 ...
```

The first snapshot appears ~2 minutes after the tiering job starts (one tiering epoch + one checkpoint). If it never appears after 5 minutes, check:

```bash
docker compose logs flink-jobmanager | grep -E "(TieringSourceEnumerator|LakeSnapshot|FinishedTiering)"
```

You should eventually see `Got FinishedTieringEvent for tiering table 0` and then `Last committed lake table snapshot info is: LakeSnapshot{snapshotId=1, ...}`.

### JM logs — tiering health

```bash
docker compose logs flink-jobmanager | grep TieringSourceEnumerator | tail -5
# Healthy: currentFinishedTables: {0=...}, tieringTableEpochs: {}
# Still running: tieringTableEpochs: {0=7}  (epoch number)
# Stuck: same epoch number for >5 minutes
```

---

## Running Benchmarks

### P2 — Union Read (hot + cold)

```bash
make flink-p2
```

Runs `docker/flink/sql/benchmark_p2.sql` against `analytics.transactions` in the Fluss catalog. Q1 (regional aggregate), Q2 (tumbling windows), Q3 (freshness probe) — each query sees both the Arrow hot layer and Parquet cold layer.

### P3 — Cold lake only

```bash
make flink-p3
```

Runs `docker/flink/sql/benchmark_p3.sql` against `` `analytics.transactions$lake` ``. The `$lake` suffix is Fluss-specific — it exposes only the Paimon cold layer, bypassing the hot Arrow layer entirely. Requires at least one committed Paimon snapshot (see above).

**Allow ~60–120s after first snapshot before running P3.**

### Interactive SQL

```bash
make flink-sql
# Opens sql-client connected to SQL Gateway at localhost:8083
```

---

## Flink SQL Dialect Notes

Several standard SQL constructs work differently in Flink SQL:

| Concept | Standard / ClickHouse | Flink SQL |
|---|---|---|
| Millisecond diff | `DATEDIFF('millisecond', a, b)` | `TIMESTAMPDIFF(SECOND, a, b) * 1000` — `MILLISECOND` is not a valid unit |
| Tumbling window (batch) | `TUMBLE(ts, INTERVAL '1' MINUTE)` | `FLOOR(ts TO MINUTE)` |
| Current timestamp | `NOW()` | `CURRENT_TIMESTAMP` (or `NOW()`, both work) |
| Cold lake table | N/A | `` `tablename$lake` `` suffix on Fluss tables |

---

## Common Failures

### `Mkdirs failed` / tiering epochs always failing

Symptom: every epoch shows up in `currentFailedTableEpochs` immediately; JM logs show `Mkdirs failed to create .../bucket-0`.

Root cause: This is the historical failure mode from when the Paimon warehouse was stored on a Docker named volume (`paimon-warehouse`). The volume is owned by root on creation; the Flink JVM (uid 9999) could not create directories. A `paimon-init` container would `chown` it once, but `restart: "no"` containers don't re-run after `make clean` — so every fresh stack was broken.

**This is no longer an issue**: the warehouse now lives at `s3://fluss/paimon-warehouse` (MinIO), which has no filesystem ownership constraints. If you see this error on an old checkout, do `make clean && make up` to pick up the updated config.

### `NoAwsCredentialsException: Dynamic session credentials for Fluss`

Symptom: TM crashes shortly after tiering job start.

Root cause: `FlussS3DelegationTokenProvider` cannot reach AWS STS to get credentials for the `s3.assumed.role.arn`.

Fix: Ensure `s3.assumed.role.sts.endpoint: http://minio:9000` is set in all four service configs (coordinator-server, tablet-server, flink-jobmanager, flink-taskmanager). See docker-compose.yml.

### `OutOfMemoryError: Java heap space` in ingestion job

Symptom: ingestion job FAILED with OOM in TM logs.

Root cause: `taskmanager.memory.process.size` not set; JVM defaults to ~512MB heap.

Fix: `taskmanager.memory.process.size: 3500m` in TM `FLINK_PROPERTIES`. Currently in docker-compose.yml.

### `snapshot/` never appears despite Parquet files being written

Symptom: bucket-0 has data-*.parquet files but snapshot/ is empty or missing.

Root cause: checkpointing not enabled on the tiering job.

Fix: The `flink-submit-tiering` Makefile target includes `"execution.checkpointing.interval":"60000"` in the JAR run configuration. If you submitted the JAR manually without this, cancel and resubmit via `make flink-submit-tiering`.

### `IllegalStateException: Trying to access closed classloader`

Symptom: tiering job FAILED; stack trace mentions `SafetyNetWrapperClassLoader`.

Root cause: Paimon Parquet reader triggers Hadoop `Configuration` lazy loading after the Flink job classloader is closed.

Fix: `classloader.check-leaked-classloader: false` on JM and TM. Currently in docker-compose.yml.

### P3 query fails with `TimeoutException`

Symptom: `make flink-p3` immediately returns `TimeoutException` for every query.

Root cause: Most commonly, the Parquet files are very large (>50MB per file). This happens when the generator ran for a long time before the first tiering run. Very large Parquet row group reads can stall in the Paimon vectored I/O reader under memory pressure from the concurrent tiering job.

Fix:
```bash
make clean && make up
make flink-submit
# Wait ~5 minutes for first snapshot, then:
make flink-p3
```

Starting fresh keeps Parquet files small (a few MB) for the first few tiering epochs.

---

## Reset Procedure

Full reset (clears all Kafka messages, Fluss hot data, and Paimon warehouse):

```bash
make clean   # docker compose down -v — removes all named volumes
make up      # rebuilds images, restarts everything, generator resumes immediately
make flink-submit  # re-submit tiering + ingestion jobs
```

After `make up`, allow ~2 minutes before running benchmarks:
- ~30s for first Fluss log data
- ~60s for first tiering epoch
- ~30s for first Flink checkpoint to commit the Paimon snapshot

---

## File Map

```
docker/flink/
├── Dockerfile                     — JM + TM image (Fluss connector + Paimon JARs + Kafka connector)
└── sql/
    ├── 01_kafka_to_fluss.sql      — streaming ingestion job (Kafka → Fluss)
    ├── benchmark_p2.sql           — P2 queries: Union Read (hot + cold)
    └── benchmark_p3.sql           — P3 queries: cold lake only ($lake suffix)
```

Paimon warehouse (MinIO at `s3://fluss/paimon-warehouse`):
```
fluss/paimon-warehouse/analytics.db/transactions/
├── schema/schema-0
├── snapshot/
│   ├── LATEST
│   ├── snapshot-1
│   └── snapshot-N
├── manifest/
│   ├── manifest-<uuid>-0
│   └── manifest-list-<uuid>-0
└── bucket-0/
    ├── data-<uuid>-1.parquet
    └── changelog-<uuid>-0.parquet
```

Browse via MinIO console: http://localhost:9001 → `fluss` → `paimon-warehouse/`
