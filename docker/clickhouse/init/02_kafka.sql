-- Phase 2 / Pattern 1: Kafka Engine consumer + Materialized View
-- Runs once on first container start via /docker-entrypoint-initdb.d

-- Kafka Engine table — stateless cursor into the Kafka topic.
-- Does not store data itself; polls Kafka and feeds the Materialized View.
-- event_time is kept as String here; the MV converts it to DateTime64.

CREATE TABLE IF NOT EXISTS analytics.kafka_transactions
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
    kafka_group_name           = 'clickhouse-consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 4,
    kafka_max_block_size       = 65536,
    kafka_skip_broken_messages = 10;

-- Materialized View — connects Kafka Engine to MergeTree continuously.
-- Fires on every batch polled from Kafka and inserts into analytics.transactions.
-- parseDateTime64BestEffort converts "2026-05-21T22:00:54.900Z" → DateTime64(3).
-- ingest_time is not set here; it uses the DEFAULT now64(3) from the target table.

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.kafka_transactions_mv
TO analytics.transactions
AS
SELECT
    event_id,
    user_id,
    amount,
    region,
    event_type,
    parseDateTime64BestEffort(event_time) AS event_time
FROM analytics.kafka_transactions;
