#!/usr/bin/env python3
"""
Streaming benchmark probe.

Every 500ms:
  1. Reads kafka_latest_event_time — MAX(event_time) across last message on
     each partition of the 'transactions' topic. Source of truth for the probe.
  2. For each query (qa, qb, qc) × system (flink, clickhouse, starrocks):
       - Reads the system's maintained result
       - Computes system_lag_ms = kafka_latest_event_time - result_max_event_time
  3. Exposes all metrics on :8000/metrics for Prometheus scraping.

Flink results are read from compacted Kafka topics (flink_result_qa/qb/qc).
ClickHouse results are read from AggregatingMergeTree tables.
StarRocks results are re-queried from the raw transactions_s table on each tick.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import clickhouse_connect
import pymysql
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "broker:29092")
CH_HOST       = os.getenv("CH_HOST", "clickhouse")
SR_HOST       = os.getenv("SR_HOST", "starrocks")
PROBE_PORT    = int(os.getenv("PROBE_PORT", "8000"))
INTERVAL      = float(os.getenv("PROBE_INTERVAL_S", "0.5"))

# ── Prometheus metrics ─────────────────────────────────────────────────────────

KAFKA_LATEST_MS = Gauge(
    "streaming_kafka_latest_event_time_ms",
    "Epoch ms of the latest event_time seen in the Kafka transactions topic",
)
PRODUCER_LAG = Gauge(
    "streaming_producer_lag_ms",
    "NOW() - kafka_latest_event_time in ms (how far producer is behind wall clock)",
)
SYSTEM_LAG = Gauge(
    "streaming_system_lag_ms",
    "kafka_latest_event_time - system result MAX(event_time) in ms",
    ["system", "query"],
)
PROBE_DURATION = Histogram(
    "streaming_probe_duration_seconds",
    "Time taken to execute each system probe",
    ["system", "query"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

QUERIES  = ["qa", "qb", "qc"]
SYSTEMS  = ["flink", "clickhouse", "starrocks"]

# ── Timestamp helpers ──────────────────────────────────────────────────────────

def parse_ts(raw: str) -> Optional[datetime]:
    """Parse ISO-8601 string (with or without Z) → UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def epoch_ms(dt: Optional[datetime]) -> Optional[float]:
    return dt.timestamp() * 1000 if dt else None

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ── Kafka helpers ──────────────────────────────────────────────────────────────

def make_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers":  KAFKA_BROKERS,
        "group.id":           group_id,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": False,
        "socket.timeout.ms":  5000,
        "session.timeout.ms": 10000,
    })


def kafka_latest_event_time(consumer: Consumer) -> Optional[datetime]:
    """Read the last message from every partition; return MAX(event_time)."""
    topic = "transactions"
    try:
        meta = consumer.list_topics(topic, timeout=5)
        if topic not in meta.topics:
            return None

        tps = []
        for pid in meta.topics[topic].partitions:
            lo, hi = consumer.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=5, cached=False
            )
            if hi > lo:
                tps.append(TopicPartition(topic, pid, hi - 1))

        if not tps:
            return None

        consumer.assign(tps)
        max_et: Optional[datetime] = None
        seen: set = set()
        deadline = time.monotonic() + 3.0

        while len(seen) < len(tps) and time.monotonic() < deadline:
            msg = consumer.poll(0.1)
            if not msg or msg.error():
                continue
            key = (msg.topic(), msg.partition())
            if key in seen:
                continue
            seen.add(key)
            try:
                data = json.loads(msg.value())
                et = parse_ts(data.get("event_time", ""))
                if et and (max_et is None or et > max_et):
                    max_et = et
            except Exception:
                pass

        return max_et
    except Exception as e:
        log.warning("kafka source probe error: %s", e)
        return None


def flink_result_event_time(consumer: Consumer, query: str) -> Optional[datetime]:
    """Read recent messages from the Flink result topic; return MAX(max_event_time)."""
    topic = f"flink_result_{query}"
    lookback = 50  # messages per partition
    try:
        meta = consumer.list_topics(topic, timeout=5)
        if topic not in meta.topics:
            return None

        tps = []
        for pid in meta.topics[topic].partitions:
            lo, hi = consumer.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=5, cached=False
            )
            if hi > lo:
                offset = max(lo, hi - lookback)
                tps.append(TopicPartition(topic, pid, offset))

        if not tps:
            return None

        consumer.assign(tps)
        max_et: Optional[datetime] = None
        deadline = time.monotonic() + 3.0

        while time.monotonic() < deadline:
            msg = consumer.poll(0.1)
            if not msg:
                break
            if msg.error():
                continue
            try:
                data = json.loads(msg.value())
                et = parse_ts(data.get("max_event_time", ""))
                if et and (max_et is None or et > max_et):
                    max_et = et
            except Exception:
                pass

        return max_et
    except Exception as e:
        log.warning("flink %s probe error: %s", query, e)
        return None

# ── ClickHouse probes ──────────────────────────────────────────────────────────

_CH_SQL = {
    # MAX across all (possibly unmerged) partial-state rows
    "qb": "SELECT max(max_event_time) FROM analytics.streaming_qb",
    # Current and previous minute bucket
    "qa": (
        "SELECT max(max_event_time) FROM analytics.streaming_qa "
        "WHERE window_start >= now() - toIntervalMinute(2)"
    ),
    # Re-scan raw events table for last 5 minutes
    "qc": (
        "SELECT max(event_time) FROM analytics.streaming_transactions "
        "WHERE event_time >= now() - toIntervalMinute(5)"
    ),
}

def probe_clickhouse(client, query: str) -> Optional[datetime]:
    try:
        row = client.query(_CH_SQL[query]).first_row
        if not row or row[0] is None:
            return None
        val = row[0]
        if isinstance(val, datetime):
            return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
        return None
    except Exception as e:
        log.warning("clickhouse %s probe error: %s", query, e)
        return None

# ── StarRocks probes ───────────────────────────────────────────────────────────

_SR_SQL = {
    "qb": "SELECT MAX(event_time) FROM analytics.transactions_s",
    "qa": (
        "SELECT MAX(event_time) FROM analytics.transactions_s "
        "WHERE event_time >= DATE_TRUNC('minute', NOW())"
    ),
    "qc": (
        "SELECT MAX(event_time) FROM analytics.transactions_s "
        "WHERE event_time >= NOW() - INTERVAL '5' MINUTE"
    ),
}

def probe_starrocks(conn, query: str) -> Optional[datetime]:
    try:
        conn.ping(reconnect=True)
        with conn.cursor() as cur:
            cur.execute(_SR_SQL[query])
            row = cur.fetchone()
        if not row or row[0] is None:
            return None
        val = row[0]
        if isinstance(val, datetime):
            return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
        return None
    except Exception as e:
        log.warning("starrocks %s probe error: %s", query, e)
        return None

# ── Main loop ──────────────────────────────────────────────────────────────────

def connect_with_retry(connect_fn, name: str, retries: int = 20, delay: float = 5.0):
    for i in range(retries):
        try:
            conn = connect_fn()
            log.info("connected to %s", name)
            return conn
        except Exception as e:
            log.warning("waiting for %s (%d/%d): %s", name, i + 1, retries, e)
            time.sleep(delay)
    raise RuntimeError(f"could not connect to {name} after {retries} attempts")


def main():
    start_http_server(PROBE_PORT)
    log.info("metrics server started on :%d", PROBE_PORT)

    kafka_src = make_consumer("probe-kafka-source")
    flink_src = make_consumer("probe-flink-results")

    ch_client = connect_with_retry(
        lambda: clickhouse_connect.get_client(host=CH_HOST, port=8123),
        "clickhouse",
    )
    sr_conn = connect_with_retry(
        lambda: pymysql.connect(
            host=SR_HOST, port=9030, user="root", password="",
            database="analytics", connect_timeout=5, autocommit=True,
        ),
        "starrocks",
    )

    log.info("probe running — interval=%.1fs", INTERVAL)

    while True:
        tick_start = time.monotonic()

        # ── Source of truth ────────────────────────────────────────────────────
        kafka_et = kafka_latest_event_time(kafka_src)
        kafka_ms = epoch_ms(kafka_et)

        if kafka_ms is not None:
            KAFKA_LATEST_MS.set(kafka_ms)
            PRODUCER_LAG.set(epoch_ms(now_utc()) - kafka_ms)

        # ── Per-query, per-system probes ───────────────────────────────────────
        for q in QUERIES:
            # Flink
            t0 = time.monotonic()
            flink_et = flink_result_event_time(flink_src, q)
            PROBE_DURATION.labels(system="flink", query=q).observe(time.monotonic() - t0)
            if flink_et and kafka_ms is not None:
                SYSTEM_LAG.labels(system="flink", query=q).set(kafka_ms - epoch_ms(flink_et))

            # ClickHouse
            t0 = time.monotonic()
            ch_et = probe_clickhouse(ch_client, q)
            PROBE_DURATION.labels(system="clickhouse", query=q).observe(time.monotonic() - t0)
            if ch_et and kafka_ms is not None:
                SYSTEM_LAG.labels(system="clickhouse", query=q).set(kafka_ms - epoch_ms(ch_et))

            # StarRocks
            t0 = time.monotonic()
            sr_et = probe_starrocks(sr_conn, q)
            PROBE_DURATION.labels(system="starrocks", query=q).observe(time.monotonic() - t0)
            if sr_et and kafka_ms is not None:
                SYSTEM_LAG.labels(system="starrocks", query=q).set(kafka_ms - epoch_ms(sr_et))

        elapsed = time.monotonic() - tick_start
        sleep_for = max(0.0, INTERVAL - elapsed)
        if elapsed > INTERVAL:
            log.debug("probe tick took %.3fs (> %.1fs interval)", elapsed, INTERVAL)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
