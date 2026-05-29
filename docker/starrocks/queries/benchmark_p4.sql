-- Pattern 4: StarRocks external Paimon catalog benchmark queries
--
-- Reads the same cold-layer Parquet files as Flink P3, but through
-- StarRocks's vectorized MPP engine instead of a single Flink batch job.
--
-- Run via: make sr-p4
-- Or interactively: make sr → paste queries manually
--
-- Expected: freshness_lag_ms ~ same as P3 (~2min tiering epoch)
--           query_latency    ~ seconds (StarRocks MPP vs Flink's minutes)

-- ── Q1: Regional aggregate — last 1 hour ─────────────────────────────────────

SELECT
    region,
    COUNT(*)            AS cnt,
    SUM(amount)         AS total,
    AVG(amount)         AS avg_amount
FROM paimon_catalog.analytics.transactions
WHERE event_time >= NOW() - INTERVAL 1 HOUR
GROUP BY region
ORDER BY total DESC;

-- ── Q2: 1-minute tumbling windows ────────────────────────────────────────────

SELECT
    DATE_TRUNC('MINUTE', event_time)  AS window_start,
    region,
    COUNT(*)                           AS event_count,
    SUM(amount)                        AS total_amount
FROM paimon_catalog.analytics.transactions
GROUP BY window_start, region
ORDER BY window_start DESC, total_amount DESC
LIMIT 120;

-- ── Q3: Freshness probe ───────────────────────────────────────────────────────

SELECT
    MAX(event_time)                                               AS newest_event_time,
    NOW()                                                         AS query_time,
    TIMESTAMPDIFF(SECOND, MAX(event_time), NOW()) * 1000          AS freshness_lag_ms,
    TIMESTAMPDIFF(SECOND, MAX(event_time), MAX(ingest_time)) * 1000 AS pipeline_lag_ms,
    COUNT(*)                                                      AS total_rows
FROM paimon_catalog.analytics.transactions;
