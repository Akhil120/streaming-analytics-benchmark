-- Pattern 3: Flink + Fluss cold lake batch queries
--
-- The `transactions$lake` suffix bypasses the hot layer and queries ONLY the
-- compacted Parquet files in MinIO. This is a pure batch columnar scan.
--
-- Cold data availability: the Lakehouse Tiering Service flushes hot → cold
-- every `table.datalake.freshness` interval (set to 30s in 01_kafka_to_fluss.sql).
-- Allow ~60–120s after ingestion starts before cold data appears.
--
-- Run via: make flink-p3
-- Or interactively: make flink-sql → paste statements one at a time
--
-- Expected: freshness_lag_ms ≈ 30000–120000ms (cold compaction interval)

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE analytics;

SET 'execution.runtime-mode' = 'BATCH';
SET 'sql-client.execution.result-mode' = 'TABLEAU';


-- ── Q1: Regional aggregate — last 1 hour (cold lake only) ────────────────────

SELECT
    region,
    COUNT(*)     AS cnt,
    SUM(amount)  AS total,
    AVG(amount)  AS avg_amount
FROM `transactions$lake`
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY region
ORDER BY total DESC;


-- ── Q2: 1-minute tumbling windows (cold lake only) ────────────────────────────

SELECT
    FLOOR(event_time TO MINUTE) AS window_start,
    region,
    COUNT(*)     AS event_count,
    SUM(amount)  AS total_amount
FROM `transactions$lake`
GROUP BY FLOOR(event_time TO MINUTE), region
ORDER BY window_start DESC, total_amount DESC
LIMIT 120;


-- ── Q3: Freshness probe (cold lake only — measures compaction lag) ─────────────

SELECT
    MAX(event_time)                                                  AS newest_event_time,
    CURRENT_TIMESTAMP                                                AS query_time,
    TIMESTAMPDIFF(MILLISECOND, MAX(event_time), CURRENT_TIMESTAMP)  AS freshness_lag_ms,
    TIMESTAMPDIFF(MILLISECOND, MAX(event_time), MAX(ingest_time))   AS pipeline_lag_ms,
    COUNT(*)                                                         AS total_rows
FROM `transactions$lake`;
