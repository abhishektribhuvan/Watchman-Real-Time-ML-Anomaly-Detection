import json
import os
import time

import joblib
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from analysis_engine import analyze_window
from status_store import create_redis_client, save_latest_report

KAFKA_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
KAFKA_CONSUMER_GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "aiops-rca-consumer")

try:
    model = joblib.load("model.pkl")
    print("[OK] Model loaded successfully.")
except FileNotFoundError:
    print("[ERROR] 'model.pkl' not found. Run 'python train_model.py' first!")
    raise SystemExit(1)


def create_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
    """Create Kafka consumer with retry logic for when Kafka is still starting."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVERS],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id=KAFKA_CONSUMER_GROUP_ID,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            print(f"[OK] Kafka consumer connected (attempt {attempt})")
            return consumer
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka not ready, retrying in {delay}s... ({attempt}/{retries})")
            time.sleep(delay)
    raise SystemExit("[FATAL] Could not connect to Kafka after multiple attempts.")


def extract_logs(payload) -> list[str]:
    """Support both batched API payloads and legacy single-log payloads."""
    if isinstance(payload, dict):
        if isinstance(payload.get("logs"), list):
            return [str(item) for item in payload["logs"]]
        if "raw_log" in payload:
            return [str(payload["raw_log"])]
    return []


def start_streaming_pipeline():
    consumer = create_consumer()
    redis_client = create_redis_client(REDIS_URL)
    print("[START] Kafka streaming consumer started. Waiting for logs...")

    logs_buffer: list[str] = []
    window_duration = 5.0
    start_time = time.time()
    window_id = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:
                    logs_buffer.extend(extract_logs(message.value))

            if time.time() - start_time >= window_duration:
                window_id += 1
                report = analyze_window(logs_buffer, model, window_id)
                save_latest_report(redis_client, report)

                print("\n" + "=" * 60)
                print(
                    f"[TIME] Timestamp: {report.window_time} | Window Size: 5 Seconds | "
                    f"Status: {report.status}"
                )
                print(
                    f"[METRICS] Total Req: {report.total_requests} | "
                    f"5xx Errors: {report.error_count} | Avg Latency: {report.avg_latency_ms:.1f}ms"
                )
                if report.anomaly_reasons:
                    print(f"[DETAIL] {'; '.join(report.anomaly_reasons)}")
                print("=" * 60 + "\n")

                logs_buffer.clear()
                start_time = time.time()

    except KeyboardInterrupt:
        print("\nStopping streaming consumer pipeline...")
    finally:
        consumer.close()
        redis_client.close()
        print("[OK] Kafka consumer closed.")


if __name__ == "__main__":
    start_streaming_pipeline()
