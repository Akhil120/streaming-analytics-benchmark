-- Phase 3 — Kafka → Fluss streaming ingestion job
-- Submitted once by flink-sql-init container at startup.
-- Re-submit manually: make flink-submit (cancel the old job first: make flink-cancel)
--
-- What this does:
--   1. Creates a Fluss catalog pointing at our CoordinatorServer
--   2. Creates analytics.transactions table in Fluss with Union Read enabled
--      (hot layer: Arrow/RAM, cold layer: Parquet in MinIO, auto-tiered every 30s)
--   3. Creates a Kafka source table (in the default catalog, ephemeral)
--   4. Submits a continuous streaming INSERT: Kafka → Fluss

-- ── Step 1: Fluss catalog ─────────────────────────────────────────────────────

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- ── Step 2: Fluss destination table ──────────────────────────────────────────

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS analytics;

USE analytics;

CREATE TABLE IF NOT EXISTS transactions (
    event_id    STRING,
    user_id     BIGINT,
    amount      DECIMAL(12, 2),
    region      STRING,
    event_type  STRING,
    event_time  TIMESTAMP(3),
    ingest_time TIMESTAMP(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled'   = 'true',
    'table.datalake.freshness' = '30s'
);

-- ── Step 3: Kafka source (ephemeral, in the default catalog) ──────────────────

USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS kafka_transactions (
    event_id    STRING,
    user_id     BIGINT,
    amount      DECIMAL(12, 2),
    region      STRING,
    event_type  STRING,
    event_time  STRING
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'transactions',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'flink-fluss-consumer',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
);

-- ── Step 4: Streaming ingestion (continuous job) ──────────────────────────────
-- REPLACE(event_time, 'Z', '') strips the UTC 'Z' suffix before TO_TIMESTAMP.
-- CURRENT_TIMESTAMP stamps the Flink processing time as ingest_time.

INSERT INTO fluss_catalog.analytics.transactions
SELECT
    event_id,
    user_id,
    amount,
    region,
    event_type,
    TO_TIMESTAMP(REPLACE(event_time, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSS') AS event_time,
    CURRENT_TIMESTAMP AS ingest_time
FROM kafka_transactions;
