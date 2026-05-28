-- Pattern 2: Flink + Fluss Union Read benchmark queries
--
-- Union Read is AUTOMATIC — querying analytics.transactions transparently
-- combines hot data (Arrow in Fluss RAM) + cold data (Parquet in MinIO).
-- Freshness is bounded by the hot layer, which is sub-second.
--
-- Run via: make flink-p2
-- Or interactively: make flink-sql → paste statements one at a time
--
-- Expected: freshness_lag_ms ≈ 500–3000ms (hot layer)

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE analytics;

SET 'execution.runtime-mode' = 'BATCH';
SET 'sql-client.execution.result-mode' = 'TABLEAU';


-- ── Q1: Regional aggregate — last 1 hour (Union Read) ────────────────────────

SELECT
    region,
    COUNT(*)     AS cnt,
    SUM(amount)  AS total,
    AVG(amount)  AS avg_amount
FROM transactions
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY region
ORDER BY total DESC;


-- ── Q2: 1-minute tumbling windows (Union Read) ────────────────────────────────
-- FLOOR(event_time TO MINUTE) = ClickHouse toStartOfMinute() equivalent

SELECT
    FLOOR(event_time TO MINUTE) AS window_start,
    region,
    COUNT(*)     AS event_count,
    SUM(amount)  AS total_amount
FROM transactions
GROUP BY FLOOR(event_time TO MINUTE), region
ORDER BY window_start DESC, total_amount DESC
LIMIT 120;


-- ── Q3: Freshness probe (Union Read — reads newest data across hot+cold) ──────

SELECT
    MAX(event_time)                                                  AS newest_event_time,
    CURRENT_TIMESTAMP                                                AS query_time,
    TIMESTAMPDIFF(SECOND, MAX(event_time), CURRENT_TIMESTAMP) * 1000  AS freshness_lag_ms,
    TIMESTAMPDIFF(SECOND, MAX(event_time), MAX(ingest_time)) * 1000   AS pipeline_lag_ms,
    COUNT(*)                                                         AS total_rows
FROM transactions;
