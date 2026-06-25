import json
import os
import random
import time
from itertools import islice

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
KAFKA_BATCH_SIZE = int(os.environ.get("KAFKA_BATCH_SIZE", "500"))


def create_producer(retries: int = 5, delay: int = 3) -> KafkaProducer:
    """Create Kafka producer with retry logic for when Kafka is still starting."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                acks=1,
                linger_ms=20,
                compression_type="gzip",
            )
            print(f"[OK] Kafka producer connected (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka not ready, retrying in {delay}s... ({attempt}/{retries})")
            time.sleep(delay)
    raise SystemExit("[FATAL] Could not connect to Kafka after multiple attempts.")


def chunk_logs(logs: list[str], chunk_size: int) -> list[list[str]]:
    return [logs[index : index + chunk_size] for index in range(0, len(logs), chunk_size)]


def simulate_stream():
    producer = create_producer()

    print("Loading log file for streaming simulation...")
    with open("server.log", "r") as file_handle:
        skipped = 0
        for _ in islice(file_handle, 10000):
            skipped += 1
        print(f"Skipped {skipped} training lines.")

        remaining_logs = [line.rstrip("\n") for line in file_handle]

    if not remaining_logs:
        print("[WARN] No logs remaining after skipping training data.")
        return

    print(f"Loaded {len(remaining_logs)} logs. Starting real-time Kafka streaming...")

    current_index = 0
    total_logs = len(remaining_logs)

    while current_index < total_logs:
        if random.random() > 0.92:
            traffic_volume = random.randint(3000, 7000)
        else:
            traffic_volume = random.randint(20, 200)

        batch = remaining_logs[current_index : current_index + traffic_volume]
        current_index += traffic_volume

        for log_chunk in chunk_logs(batch, KAFKA_BATCH_SIZE):
            producer.send(KAFKA_TOPIC, {"logs": log_chunk, "source": "direct-kafka-producer"})
        producer.flush()
        print(f"[{time.strftime('%H:%M:%S')}] Shipped batch of {len(batch)} logs to Kafka.")
        time.sleep(1)

    producer.close()
    print("[OK] All logs streamed. Producer closed.")


if __name__ == "__main__":
    simulate_stream()
