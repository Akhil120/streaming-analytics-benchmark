-- P6: StarRocks native table + Routine Load from Kafka
-- Run once via: make sr-p6-init

CREATE DATABASE IF NOT EXISTS analytics;

-- ingest_time uses DEFAULT CURRENT_TIMESTAMP — avoids function calls in Routine Load
-- COLUMNS clause, which trigger a NPE bug in StarRocks 3.3.0 (even now()).
CREATE TABLE IF NOT EXISTS analytics.transactions_p6 (
  event_id    VARCHAR(64)    NOT NULL,
  user_id     BIGINT,
  amount      DECIMAL(12,2),
  region      VARCHAR(32),
  event_type  VARCHAR(32),
  event_time  DATETIME,
  ingest_time DATETIME       DEFAULT CURRENT_TIMESTAMP
) PRIMARY KEY (event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Routine Load: consume transactions topic from the beginning, all 12 partitions.
-- No function calls in COLUMNS — StarRocks 3.3.0 NPEs on any function expression
-- (including now()) in the COLUMNS clause of Routine Load. ingest_time is populated
-- by its DEFAULT CURRENT_TIMESTAMP when the column is omitted from the load columns.
CREATE ROUTINE LOAD analytics.transactions_p6_load ON transactions_p6
COLUMNS(event_id, user_id, amount, region, event_type, event_time)
PROPERTIES (
  "desired_concurrent_number" = "12",
  "max_batch_interval"        = "5",
  "max_batch_rows"            = "300000",
  "max_batch_size"            = "209715200",
  "format"                    = "json",
  "jsonpaths"                 = '["$.event_id","$.user_id","$.amount","$.region","$.event_type","$.event_time"]',
  "strict_mode"               = "false"
)
FROM KAFKA (
  "kafka_broker_list"          = "broker:29092",
  "kafka_topic"                = "transactions",
  "property.group.id"          = "starrocks-routine-load-p6",
  "property.auto.offset.reset" = "earliest"
);
