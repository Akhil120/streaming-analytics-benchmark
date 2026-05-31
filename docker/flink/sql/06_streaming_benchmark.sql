-- Streaming benchmark — Flink SQL
-- Three continuous queries submitted as a single STATEMENT SET (shared Kafka source).
-- Results written to compacted Kafka topics; probe reads latest value per key.
--
-- Submit via: make stream-flink-submit
-- Cancel via: make flink-cancel  (then resubmit as needed)
--
-- Prerequisites:
--   1. Cancel existing Flink jobs (make flink-cancel) to free task slots.
--   2. flink_result_qa / qb / qc topics must exist (created by kafka-init).

-- ── Production-grade streaming config ────────────────────────────────────────

SET 'parallelism.default'                                    = '2';
SET 'execution.buffer-timeout'                               = '1 ms';
SET 'state.backend'                                          = 'rocksdb';
SET 'state.backend.incremental'                              = 'true';
SET 'execution.checkpointing.interval'                       = '30 s';
SET 'execution.checkpointing.mode'                           = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause'                      = '10 s';
SET 'execution.checkpointing.timeout'                        = '60 s';
SET 'execution.checkpointing.tolerable-failed-checkpoints'   = '3';

-- ── Kafka source ──────────────────────────────────────────────────────────────
-- event_time kept as STRING to handle trailing Z (Flink ISO-8601 parser bug).
-- proc_time provides processing-time attribute for window functions.

CREATE TABLE IF NOT EXISTS kafka_source_s (
    event_id    STRING,
    user_id     BIGINT,
    amount      DECIMAL(12, 2),
    region      STRING,
    event_type  STRING,
    event_time  STRING,
    proc_time   AS PROCTIME()
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'transactions',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'flink-streaming-benchmark',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
);

-- ── Result sinks (upsert-kafka, compacted topics) ─────────────────────────────

-- Q-B sink: keyed by region
CREATE TABLE IF NOT EXISTS flink_result_qb (
    region          STRING,
    event_count     BIGINT,
    total_amount    DECIMAL(12, 2),
    max_event_time  STRING,
    PRIMARY KEY (region) NOT ENFORCED
) WITH (
    'connector'                     = 'upsert-kafka',
    'topic'                         = 'flink_result_qb',
    'properties.bootstrap.servers'  = 'broker:29092',
    'key.format'                    = 'json',
    'value.format'                  = 'json'
);

-- Q-A sink: keyed by (window_start, region)
CREATE TABLE IF NOT EXISTS flink_result_qa (
    window_start    TIMESTAMP(3),
    region          STRING,
    event_count     BIGINT,
    total_amount    DECIMAL(12, 2),
    max_event_time  STRING,
    PRIMARY KEY (window_start, region) NOT ENFORCED
) WITH (
    'connector'                     = 'upsert-kafka',
    'topic'                         = 'flink_result_qa',
    'properties.bootstrap.servers'  = 'broker:29092',
    'key.format'                    = 'json',
    'value.format'                  = 'json'
);

-- Q-C sink: keyed by region (latest 5-min aggregate per region)
CREATE TABLE IF NOT EXISTS flink_result_qc (
    region          STRING,
    event_count     BIGINT,
    total_amount    DECIMAL(12, 2),
    max_event_time  STRING,
    PRIMARY KEY (region) NOT ENFORCED
) WITH (
    'connector'                     = 'upsert-kafka',
    'topic'                         = 'flink_result_qc',
    'properties.bootstrap.servers'  = 'broker:29092',
    'key.format'                    = 'json',
    'value.format'                  = 'json'
);

-- ── All three queries in one job (shared source, single checkpoint barrier) ───

EXECUTE STATEMENT SET
BEGIN

  -- Q-B: running total per region (unbounded state, 7 keys)
  INSERT INTO flink_result_qb
  SELECT
      region,
      COUNT(*)        AS event_count,
      SUM(amount)     AS total_amount,
      MAX(event_time) AS max_event_time
  FROM kafka_source_s
  GROUP BY region;

  -- Q-A: tumbling 1-minute window (processing time)
  -- Window closes at each minute boundary; final result emitted once per window.
  INSERT INTO flink_result_qa
  SELECT
      TUMBLE_START(proc_time, INTERVAL '1' MINUTE)  AS window_start,
      region,
      COUNT(*)                                       AS event_count,
      SUM(amount)                                    AS total_amount,
      MAX(event_time)                                AS max_event_time
  FROM kafka_source_s
  GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE), region;

  -- Q-C: last 5 minutes per region (OVER window, incremental state)
  -- Emits one result per input event; upsert-kafka retains latest per region.
  -- State size: all events within 5-minute range per region (RocksDB-backed).
  INSERT INTO flink_result_qc
  SELECT
      region,
      COUNT(event_id) OVER w  AS event_count,
      SUM(amount)     OVER w  AS total_amount,
      MAX(event_time) OVER w  AS max_event_time
  FROM kafka_source_s
  WINDOW w AS (
      PARTITION BY region
      ORDER BY proc_time
      RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
  );

END;
