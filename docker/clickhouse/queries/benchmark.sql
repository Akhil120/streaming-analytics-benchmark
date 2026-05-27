-- Pattern 1 — ClickHouse benchmark queries
-- Run against: clickhouse-client --database analytics
-- Or via HTTP: curl "http://localhost:8123/?query=..." --data-binary @benchmark.sql

-- ── Q1: Regional aggregate — last 1 hour ──────────────────────────────────────
-- Measures: query_latency (wall time), data coverage (freshness bounded by ingestion)

SELECT
    region,
    count()         AS cnt,
    sum(amount)     AS total,
    avg(amount)     AS avg_amount
FROM analytics.transactions
WHERE event_time >= now64(3) - INTERVAL 1 HOUR
GROUP BY region
ORDER BY total DESC;


-- ── Q2: 1-minute tumbling windows ─────────────────────────────────────────────
-- ClickHouse uses toStartOfMinute() — equivalent to TUMBLE(event_time, INTERVAL 1 MINUTE)

SELECT
    toStartOfMinute(event_time) AS window_start,
    region,
    count()                     AS event_count,
    sum(amount)                 AS total_amount
FROM analytics.transactions
GROUP BY window_start, region
ORDER BY window_start DESC, total_amount DESC
LIMIT 120;  -- last ~10 minutes of 1-min buckets across 7 regions


-- ── Q3: Freshness probe — run every 5 seconds ─────────────────────────────────
-- Core benchmark metric. Compare freshness_lag_ms across all 6 patterns.
--
--   freshness_lag_ms  = how stale is the newest data this query can see
--   pipeline_lag_ms   = how long the Kafka → MergeTree ingestion path took
--
-- Expected for Pattern 1: freshness_lag_ms ≈ 1000–5000ms (kafka_flush_interval_ms)

SELECT
    max(event_time)                                              AS newest_event_time,
    now64(3)                                                     AS query_time,
    dateDiff('millisecond', max(event_time), now64(3))          AS freshness_lag_ms,
    dateDiff('second',      max(event_time), now64(3))          AS freshness_lag_seconds,
    max(ingest_time)                                             AS newest_ingest_time,
    dateDiff('millisecond', max(event_time), max(ingest_time))  AS pipeline_lag_ms,
    count()                                                      AS total_rows
FROM analytics.transactions;


-- ── Ingestion monitoring ───────────────────────────────────────────────────────
-- Useful for verifying the Kafka Engine is consuming and data is flowing.

-- Messages ingested per second (last 5 minutes, bucketed by minute)
SELECT
    toStartOfMinute(ingest_time) AS minute,
    count()                      AS messages_ingested,
    round(count() / 60, 1)       AS msgs_per_sec
FROM analytics.transactions
WHERE ingest_time >= now() - INTERVAL 5 MINUTE
GROUP BY minute
ORDER BY minute DESC;

-- Consumer group lag check (run from Kafka side, not ClickHouse)
-- docker compose exec broker kafka-consumer-groups \
--   --bootstrap-server broker:29092 \
--   --describe --group clickhouse-consumer

-- ── System health ──────────────────────────────────────────────────────────────

-- Row count and size on disk
SELECT
    table,
    formatReadableQuantity(sum(rows))                   AS rows,
    formatReadableSize(sum(data_compressed_bytes))      AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))    AS uncompressed,
    round(sum(data_uncompressed_bytes) /
          sum(data_compressed_bytes), 2)                AS ratio
FROM system.parts
WHERE database = 'analytics' AND active
GROUP BY table;

-- Kafka Engine consumer status
SELECT
    database, table, consumer_id, num_messages_processed, last_exception
FROM system.kafka_consumers
WHERE database = 'analytics';
