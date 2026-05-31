-- Streaming benchmark — ClickHouse setup
-- Separate consumer group from P1 so benchmark 1 offsets are unaffected.
-- Three objects maintained:
--   streaming_transactions  — raw MergeTree (used by Q-C re-scans)
--   streaming_qb            — AggregatingMergeTree per-region running total (Q-B)
--   streaming_qa            — AggregatingMergeTree per-minute window (Q-A)

-- ── Kafka source (dedicated consumer group, tuned for low latency) ─────────────

CREATE TABLE IF NOT EXISTS analytics.kafka_transactions_s
(
    event_id    String,
    user_id     UInt64,
    amount      Decimal(12, 2),
    region      String,
    event_type  String,
    event_time  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'broker:29092',
    kafka_topic_list           = 'transactions',
    kafka_group_name           = 'ch-streaming',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 4,
    kafka_max_block_size       = 65536,
    kafka_flush_interval_ms    = 500,
    kafka_skip_broken_messages = 10;

-- ── Raw events table (for Q-C range scans) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.streaming_transactions
(
    event_id    String,
    user_id     UInt64,
    amount      Decimal(12, 2),
    region      LowCardinality(String),
    event_type  LowCardinality(String),
    event_time  DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDDhh(event_time)
ORDER BY (region, event_time)
TTL toDateTime(event_time) + INTERVAL 2 HOUR
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.streaming_transactions_mv
TO analytics.streaming_transactions
AS
SELECT
    event_id,
    user_id,
    amount,
    region,
    event_type,
    parseDateTime64BestEffort(event_time) AS event_time
FROM analytics.kafka_transactions_s;

-- ── Q-B: running total per region (AggregatingMergeTree) ─────────────────────
-- Probe query: SELECT region, sum(event_count), sum(total_amount), max(max_event_time)
--              FROM analytics.streaming_qb GROUP BY region

CREATE TABLE IF NOT EXISTS analytics.streaming_qb
(
    region          LowCardinality(String),
    event_count     SimpleAggregateFunction(sum, UInt64),
    total_amount    SimpleAggregateFunction(sum, Decimal(12, 2)),
    max_event_time  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY region;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.streaming_qb_mv
TO analytics.streaming_qb
AS
SELECT
    region,
    count()                             AS event_count,
    sum(amount)                         AS total_amount,
    max(parseDateTime64BestEffort(event_time)) AS max_event_time
FROM analytics.kafka_transactions_s
GROUP BY region;

-- ── Q-A: tumbling 1-minute window (AggregatingMergeTree) ─────────────────────
-- Probe query: SELECT window_start, region, sum(event_count), max(max_event_time)
--              FROM analytics.streaming_qa
--              WHERE window_start >= now() - toIntervalMinute(2)
--              GROUP BY window_start, region

CREATE TABLE IF NOT EXISTS analytics.streaming_qa
(
    window_start    DateTime('UTC'),
    region          LowCardinality(String),
    event_count     SimpleAggregateFunction(sum, UInt64),
    total_amount    SimpleAggregateFunction(sum, Decimal(12, 2)),
    max_event_time  SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (window_start, region)
TTL window_start + INTERVAL 2 HOUR;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.streaming_qa_mv
TO analytics.streaming_qa
AS
SELECT
    toStartOfMinute(parseDateTime64BestEffort(event_time)) AS window_start,
    region,
    count()                                                 AS event_count,
    sum(amount)                                             AS total_amount,
    max(parseDateTime64BestEffort(event_time))              AS max_event_time
FROM analytics.kafka_transactions_s
GROUP BY window_start, region;
