-- Phase 2 / Pattern 1: ClickHouse target table
-- Runs once on first container start via /docker-entrypoint-initdb.d

CREATE DATABASE IF NOT EXISTS analytics;

-- Target table — receives data from the Kafka Engine via Materialized View.
-- Two timestamp columns:
--   event_time  = when the event was produced to Kafka (embedded by generator)
--   ingest_time = when ClickHouse committed it to MergeTree (set by DEFAULT)
-- freshness_lag = now() - MAX(event_time)   → how stale is the data a query sees
-- pipeline_lag  = ingest_time - event_time  → how long the ingestion path took

CREATE TABLE IF NOT EXISTS analytics.transactions
(
    event_id    String,
    user_id     UInt64,
    amount      Decimal(12, 2),
    region      LowCardinality(String),
    event_type  LowCardinality(String),
    event_time  DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (region, event_time, event_id)
SETTINGS index_granularity = 8192;
