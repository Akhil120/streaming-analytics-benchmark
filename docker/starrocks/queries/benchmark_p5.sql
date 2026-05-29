-- P5 benchmark: StarRocks native PK table fed by Flink streaming write from Fluss
-- Run via: make sr-p5

-- Q1: regional aggregate — last 1 hour
SELECT   region,
         COUNT(*)      AS cnt,
         SUM(amount)   AS total,
         AVG(amount)   AS avg_amount
FROM     analytics.transactions_p5
WHERE    event_time >= NOW() - INTERVAL 1 HOUR
GROUP BY region
ORDER BY total DESC;

-- Q2: 1-minute tumbling windows (full dataset)
SELECT   DATE_TRUNC('MINUTE', event_time) AS window_start,
         region,
         COUNT(*)      AS event_count,
         SUM(amount)   AS total_amount
FROM     analytics.transactions_p5
GROUP BY window_start, region
ORDER BY window_start DESC, total_amount DESC
LIMIT    120;

-- Q3: freshness probe
SELECT   MAX(event_time)                                                  AS newest_event_time,
         NOW()                                                            AS query_time,
         TIMESTAMPDIFF(SECOND, MAX(event_time), NOW())        * 1000     AS freshness_lag_ms,
         TIMESTAMPDIFF(SECOND, MAX(event_time), MAX(ingest_time)) * 1000 AS pipeline_lag_ms,
         COUNT(*)                                                         AS total_rows
FROM     analytics.transactions_p5;
