-- Streaming benchmark — StarRocks setup
-- Separate table and consumer group from P6 so benchmark 1 is unaffected.
-- transactions_s is the single target for all three probe queries (Q-A, Q-B, Q-C).
-- The probe re-aggregates on each tick — no maintained aggregate tables on the SR side.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.transactions_s (
    event_id    VARCHAR(64)   NOT NULL,
    user_id     BIGINT,
    amount      DECIMAL(12, 2),
    region      VARCHAR(32),
    event_type  VARCHAR(32),
    event_time  DATETIME
) PRIMARY KEY (event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Tuned Routine Load: max_batch_interval=1 (minimum scheduler cadence),
-- desired_concurrent_number=4 (4 tasks across 12 partitions).
-- property.auto.offset.reset=latest: consume only new data, not history.

CREATE ROUTINE LOAD analytics.transactions_s_load ON transactions_s
COLUMNS TERMINATED BY ",",
COLUMNS (event_id, user_id, amount, region, event_type, event_time)
PROPERTIES (
    "desired_concurrent_number" = "4",
    "max_batch_interval"        = "1",
    "max_batch_rows"            = "100000",
    "max_batch_size"            = "104857600",
    "format"                    = "json",
    "jsonpaths"                 = '["$.event_id","$.user_id","$.amount","$.region","$.event_type","$.event_time"]',
    "strict_mode"               = "false"
)
FROM KAFKA (
    "kafka_broker_list"          = "broker:29092",
    "kafka_topic"                = "transactions",
    "property.group.id"          = "starrocks-streaming",
    "property.auto.offset.reset" = "latest"
);
