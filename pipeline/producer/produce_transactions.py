"""Simulates a stream of card-present/card-not-present financial transactions
and publishes each one as JSON to a Kafka topic.

A small pool of accounts is reused across events (rather than generating a
fresh account per transaction) so downstream velocity/aggregation logic has
something meaningful to compute per account_id.

Two modes, controlled by PRODUCER_MODE:
  - "stream" (default): runs forever at ~RATE_PER_SEC, the live-feed simulation.
  - "batch": sends PRODUCER_BATCH_SIZE messages as fast as possible, then
    exits. Meant for quickly seeding data to test Spark/dbt without waiting
    minutes for the live stream to accumulate rows.
"""
import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
RATE_PER_SEC = float(os.environ.get("PRODUCER_RATE_PER_SEC", "5"))
NUM_ACCOUNTS = int(os.environ.get("PRODUCER_NUM_ACCOUNTS", "25"))
MODE = os.environ.get("PRODUCER_MODE", "stream").lower()  # "stream" | "batch"
BATCH_SIZE = int(os.environ.get("PRODUCER_BATCH_SIZE", "1000"))

CATEGORIES = [
    "groceries", "dining", "travel", "electronics", "utilities",
    "entertainment", "healthcare", "fuel", "subscription", "cash_withdrawal",
]

ACCOUNTS = [f"acct_{i:04d}" for i in range(1, NUM_ACCOUNTS + 1)]


def build_transaction() -> dict:
    category = random.choice(CATEGORIES)
    # cash withdrawals / travel skew higher; everyday categories skew lower
    base = {
        "cash_withdrawal": (50, 500),
        "travel": (80, 1200),
        "electronics": (40, 900),
    }.get(category, (5, 150))

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNTS),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(*base), 2),
        "merchant": fake.company(),
        "category": category,
        "location": fake.city(),
        "card_present": random.random() > 0.35,
    }


def on_send_error(excp):
    """errback for producer.send() — without this, failed sends vanish silently
    and the 'sent' counter keeps incrementing as if delivery succeeded."""
    print(f"[producer] send failed: {excp!r}")


def request_shutdown(signum, frame):
    """docker compose down sends SIGTERM, not SIGINT, so without this handler
    the KeyboardInterrupt catch below never fires and buffered messages are
    dropped instead of flushed."""
    raise KeyboardInterrupt


def connect_with_retry(max_attempts: int = 15, delay_sec: int = 5) -> KafkaProducer:
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{max_attempts}), retrying in {delay_sec}s...")
            time.sleep(delay_sec)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def run_stream(producer: KafkaProducer) -> None:
    print(f"[producer] connected to {BOOTSTRAP_SERVERS}, streaming to topic '{TOPIC}' at ~{RATE_PER_SEC}/sec")

    interval = 1.0 / RATE_PER_SEC
    sent = 0
    try:
        while True:
            txn = build_transaction()
            producer.send(TOPIC, key=txn["account_id"], value=txn).add_errback(on_send_error)
            sent += 1
            if sent % 50 == 0:
                producer.flush()
                print(f"[producer] sent {sent} transactions")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] shutting down, flushing buffered messages...")
    finally:
        producer.flush()
        producer.close()


def run_batch(producer: KafkaProducer) -> None:
    print(f"[producer] connected to {BOOTSTRAP_SERVERS}, batch mode: sending {BATCH_SIZE} transactions to '{TOPIC}'")

    try:
        for sent in range(1, BATCH_SIZE + 1):
            txn = build_transaction()
            producer.send(TOPIC, key=txn["account_id"], value=txn).add_errback(on_send_error)
            if sent % 500 == 0:
                producer.flush()
                print(f"[producer] sent {sent}/{BATCH_SIZE}")
    except KeyboardInterrupt:
        print("[producer] batch interrupted, flushing buffered messages...")
    finally:
        producer.flush()
        producer.close()
    print("[producer] batch complete")


def main():
    signal.signal(signal.SIGTERM, request_shutdown)

    producer = connect_with_retry()

    if MODE == "batch":
        run_batch(producer)
    else:
        run_stream(producer)


if __name__ == "__main__":
    main()
