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

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "aiops-rca-consumer")

MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "ml", "model.pkl")

try:
    model = joblib.load(MODEL_PATH)
except FileNotFoundError:
    raise SystemExit(1)

def _create_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
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
            return consumer
        except NoBrokersAvailable:
            time.sleep(delay)
    raise SystemExit("Could not connect to Kafka.")

def _extract_logs(payload) -> list[str]:
    if isinstance(payload, dict):
        if isinstance(payload.get("logs"), list):
            return [str(item) for item in payload["logs"]]
        if "raw_log" in payload:
            return [str(payload["raw_log"])]
    return []

def run():
    consumer = _create_consumer()
    redis = create_redis_client(REDIS_URL)

    buffer: list[str] = []
    window_start = time.time()
    window_id = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for msg in messages:
                    buffer.extend(_extract_logs(msg.value))

            if time.time() - window_start >= 5.0:
                window_id += 1
                report = analyze_window(buffer, model, window_id)
                save_report(redis, report)

                buffer.clear()
                window_start = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        redis.close()

if __name__ == "__main__":
    run()
