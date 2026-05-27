#!/usr/bin/env python3
"""
High-volume Kafka producer for the analytics benchmark.

Every message embeds event_time (millisecond-precision UTC) so all downstream
systems can measure:
  freshness_lag        = NOW() - MAX(event_time)    (how stale is the newest visible data)
  query_latency        = wall time from submit to last row
  result_update_latency = produce time to result visible (streaming patterns only)

Environment variables:
  KAFKA_BROKERS   comma-separated brokers  (default: localhost:9092)
  TOPIC           target topic             (default: transactions)
  TARGET_RPS      msgs/sec, 0 = unlimited  (default: 100000)
  NUM_THREADS     producer threads         (default: 8)
  ACKS            1 | all                  (default: 1)
  COMPRESSION     lz4 | snappy | gzip      (default: lz4)
  LINGER_MS       batch linger ms          (default: 5)
  BATCH_SIZE      max batch bytes          (default: 131072)
"""

import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

# ── Config ─────────────────────────────────────────────────────────────────────

BROKERS     = os.getenv("KAFKA_BROKERS", "localhost:9092")
TOPIC       = os.getenv("TOPIC", "transactions")
TARGET_RPS  = int(os.getenv("TARGET_RPS", "100000"))
NUM_THREADS = int(os.getenv("NUM_THREADS", "8"))
ACKS        = os.getenv("ACKS", "1")
COMPRESSION = os.getenv("COMPRESSION", "lz4")
LINGER_MS   = int(os.getenv("LINGER_MS", "5"))
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "131072"))

# ── Reference data ─────────────────────────────────────────────────────────────

REGIONS     = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1",
               "ap-southeast-1", "ap-northeast-1", "sa-east-1"]
EVENT_TYPES = ["purchase", "refund", "view", "add_to_cart", "checkout", "login"]


def make_event() -> tuple[bytes, bytes]:
    """Return (key, value) as bytes. Key = user_id for partition affinity."""
    now = datetime.now(timezone.utc)
    user_id = random.randint(1, 10_000_000)
    event = {
        "event_id":   str(uuid.uuid4()),
        "user_id":    user_id,
        "amount":     round(random.uniform(0.01, 9_999.99), 2),
        "region":     random.choice(REGIONS),
        "event_type": random.choice(EVENT_TYPES),
        # ISO-8601 ms-precision UTC — core field for freshness_lag measurement
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
    }
    return str(user_id).encode(), json.dumps(event, separators=(",", ":")).encode()


# ── Producer thread ────────────────────────────────────────────────────────────

class ProducerThread(threading.Thread):
    def __init__(self, thread_id: int, rps_limit: int):
        super().__init__(daemon=True, name=f"producer-{thread_id}")
        self.rps_limit = rps_limit
        self.sent      = 0
        self.errors    = 0

        self._p = Producer({
            "bootstrap.servers":            BROKERS,
            "acks":                         ACKS,
            "compression.type":             COMPRESSION,
            "linger.ms":                    LINGER_MS,
            "batch.size":                   BATCH_SIZE,
            "queue.buffering.max.messages": 2_000_000,
            "queue.buffering.max.kbytes":   2_097_152,   # 2 GB in-flight buffer
            "socket.keepalive.enable":      True,
        })

    def _on_delivery(self, err, _msg):
        if err:
            self.errors += 1

    def run(self):
        # Batch-based rate limiting: sleep once per BATCH_CHECK messages rather than
        # per-message. This avoids macOS sleep precision issues at sub-ms intervals
        # (per-message sleep at 80µs rounds up to ~1ms, capping throughput at ~1K/thread).
        BATCH_CHECK  = 500
        target_batch = (BATCH_CHECK / self.rps_limit) if self.rps_limit > 0 else 0.0
        batch_start  = time.monotonic()

        while True:
            for _ in range(BATCH_CHECK):
                key, payload = make_event()
                self._p.produce(TOPIC, key=key, value=payload, on_delivery=self._on_delivery)
                self.sent += 1

            self._p.poll(0)   # drain delivery callbacks after each batch

            if target_batch > 0:
                elapsed = time.monotonic() - batch_start
                slack   = target_batch - elapsed
                if slack > 0:
                    time.sleep(slack)
                batch_start = time.monotonic()


# ── Stats reporter ─────────────────────────────────────────────────────────────

def stats_reporter(threads: list):
    prev_sent   = [0] * len(threads)
    prev_errors = [0] * len(threads)

    while True:
        time.sleep(1)
        curr_sent   = [t.sent   for t in threads]
        curr_errors = [t.errors for t in threads]

        rps   = sum(c - p for c, p in zip(curr_sent,   prev_sent))
        erps  = sum(c - p for c, p in zip(curr_errors, prev_errors))
        total = sum(curr_sent)

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}]"
            f"  rps: {rps:>9,}"
            f"  errors/s: {erps:>3}"
            f"  total: {total:>13,}",
            flush=True,
        )

        prev_sent   = curr_sent
        prev_errors = curr_errors


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    rps_per_thread = (TARGET_RPS // NUM_THREADS) if TARGET_RPS > 0 else 0
    limit_label    = f"{TARGET_RPS:,}" if TARGET_RPS > 0 else "unlimited"

    print(f"brokers     : {BROKERS}")
    print(f"topic       : {TOPIC}")
    print(f"threads     : {NUM_THREADS}")
    print(f"target rps  : {limit_label}  ({rps_per_thread:,} / thread)")
    print(f"acks        : {ACKS}  compression: {COMPRESSION}  "
          f"linger: {LINGER_MS}ms  batch: {BATCH_SIZE // 1024}KB")
    print("-" * 60)

    threads = [ProducerThread(i, rps_per_thread) for i in range(NUM_THREADS)]
    for t in threads:
        t.start()

    stats_reporter(threads)   # blocks forever


if __name__ == "__main__":
    main()
