-- P5: Flink streaming write — Kafka → Flink → StarRocks PK table
-- Source: Kafka transactions topic (same source as P1/P6, separate consumer group)
-- Sink:   StarRocks analytics.transactions_p5 via Stream Load V1 (direct to BE HTTP port)
--
-- Source is Kafka (not Fluss) for two reasons:
-- 1. Avoids changelog stream semantics (INSERT/UPDATE_BEFORE/UPDATE_AFTER) from Fluss
--    PK tables, which interact badly with the V1 StarRocks connector's JSON serializer.
-- 2. Cleaner comparison: isolates the Flink → StarRocks write path from Fluss behavior.
--
-- V1 Stream Load connects directly to starrocks:8040 (BE HTTP).
-- V2 (default) causes FE to redirect to the BE's self-advertised address
-- (127.0.0.1:8040 inside the allin1 container), unreachable from Flink's network.
--
-- CSV format avoids JSON type-parsing edge cases (TIMESTAMP serialization, DECIMAL quoting).
-- \x01 (SOH) is used as the column separator — safe for text payloads.
--
-- event_time is read as STRING to handle the 'Z' UTC suffix in ISO-8601 timestamps
-- (e.g. "2026-05-29T00:17:49.301Z"). Flink's json.timestamp-format.standard=ISO-8601
-- does not strip the trailing 'Z' in this version. Cast is done in the INSERT using
-- TO_TIMESTAMP(REPLACE(..., 'Z', ''), ...) — same approach as 01_kafka_to_fluss.sql.
--
-- Submit via: make flink-submit-p5

-- Parallelism 1: single stream load at a time, avoids concurrent FE transaction commits
-- that cause THRIFT_EAGAIN timeouts in StarRocks 3.3.0 allin1.
SET 'parallelism.default'              = '1';
-- No checkpointing: checkpoint-triggered forced flushes during catch-up cause large
-- back-to-back batches that overwhelm FE. For benchmarking, time-based flushes suffice.
SET 'execution.checkpointing.interval' = '99999999ms';

-- Kafka source: separate consumer group so it doesn't interfere with the P1 ClickHouse
-- consumer or the Flink→Fluss ingestion job.
-- event_time declared as STRING — cast to TIMESTAMP in the INSERT below.
CREATE TABLE IF NOT EXISTS kafka_transactions_p5 (
    event_id   STRING,
    user_id    BIGINT,
    amount     DECIMAL(12, 2),
    region     STRING,
    event_type STRING,
    event_time STRING
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'transactions',
    'properties.bootstrap.servers'      = 'broker:29092',
    'properties.group.id'               = 'flink-starrocks-p5',
    'scan.startup.mode'                 = 'latest-offset',
    'format'                            = 'json',
    'json.fail-on-missing-field'        = 'false',
    'json.ignore-parse-errors'          = 'true'
);

-- StarRocks sink: V1 Stream Load direct to BE, CSV format
CREATE TABLE IF NOT EXISTS starrocks_p5_sink (
    event_id    STRING,
    user_id     BIGINT,
    amount      DECIMAL(12, 2),
    region      STRING,
    event_type  STRING,
    event_time  TIMESTAMP(3),
    ingest_time TIMESTAMP(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector'                         = 'starrocks',
    'jdbc-url'                          = 'jdbc:mysql://starrocks:9030',
    'load-url'                          = 'starrocks:8040',
    'database-name'                     = 'analytics',
    'table-name'                        = 'transactions_p5',
    'username'                          = 'root',
    'password'                          = '',
    'sink.version'                      = 'V1',
    'sink.buffer-flush.interval-ms'     = '5000',
    'sink.buffer-flush.max-rows'        = '64000',
    'sink.properties.format'            = 'csv',
    'sink.properties.column_separator'  = '\x01'
);

INSERT INTO starrocks_p5_sink
SELECT
    event_id,
    user_id,
    amount,
    region,
    event_type,
    TO_TIMESTAMP(REPLACE(event_time, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSS') AS event_time,
    CURRENT_TIMESTAMP AS ingest_time
FROM kafka_transactions_p5;
