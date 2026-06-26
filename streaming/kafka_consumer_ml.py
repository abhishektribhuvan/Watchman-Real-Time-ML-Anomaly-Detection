"""Kafka consumer — reads raw logs, runs ML analysis, saves results to Redis."""

import json
import logging
import os
import time

import joblib
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from ml.analysis_engine import analyze_window
from app.status_store import create_redis_client, save_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("consumer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "aiops-rca-consumer")

# ---------------------------------------------------------------------------
# Load ML model
# ---------------------------------------------------------------------------
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "ml", "model.pkl")

try:
    model = joblib.load(MODEL_PATH)
    log.info("ML model loaded from %s", MODEL_PATH)
except FileNotFoundError:
    log.error("model.pkl not found at %s — run 'python -m ml.train_model' first", MODEL_PATH)
    raise SystemExit(1)


def _create_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
    """Create Kafka consumer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVERS],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=GROUP_ID,
                value_deserializer=lambda raw: json.loads(raw.decode()),
            )
            log.info("Kafka consumer connected (attempt %d)", attempt)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready — retrying in %ds (%d/%d)", delay, attempt, retries)
            time.sleep(delay)
    raise SystemExit("Could not connect to Kafka.")


def _extract_logs(payload) -> list[str]:
    """Support both batched API payloads and single-log payloads."""
    if isinstance(payload, dict):
        if isinstance(payload.get("logs"), list):
            return [str(item) for item in payload["logs"]]
        if "raw_log" in payload:
            return [str(payload["raw_log"])]
    return []


def run():
    """Main consumer loop — polls Kafka, analyzes in 5s windows, saves to Redis."""
    consumer = _create_consumer()
    redis = create_redis_client(REDIS_URL)
    log.info("Streaming pipeline started — waiting for logs...")

    buffer: list[str] = []
    window_start = time.time()
    window_id = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for msg in messages:
                    buffer.extend(_extract_logs(msg.value))

            # Flush the window every 5 seconds
            if time.time() - window_start >= 5.0:
                window_id += 1
                report = analyze_window(buffer, model, window_id)
                save_report(redis, report)

                log.info(
                    "Window %d | %d logs | status=%s | errors=%d | latency=%.0fms",
                    window_id,
                    report.total_requests,
                    report.status,
                    report.error_count,
                    report.avg_latency_ms,
                )

                buffer.clear()
                window_start = time.time()

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        consumer.close()
        redis.close()


if __name__ == "__main__":
    run()
