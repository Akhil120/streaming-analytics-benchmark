-- P5: StarRocks native PK table — receives streaming writes from Flink
-- Run once via: make sr-p5-init

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.transactions_p5 (
  event_id    VARCHAR(64)   NOT NULL,
  user_id     BIGINT,
  amount      DECIMAL(12,2),
  region      VARCHAR(32),
  event_type  VARCHAR(32),
  event_time  DATETIME,
  ingest_time DATETIME
) PRIMARY KEY (event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
