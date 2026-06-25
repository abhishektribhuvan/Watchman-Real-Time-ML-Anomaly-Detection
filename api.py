import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from schemas import LogBatch, WindowReport
from status_store import create_redis_client, load_latest_report

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
KAFKA_BATCH_SIZE = int(os.environ.get("KAFKA_BATCH_SIZE", "500"))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

producer = None
redis_client = None


def create_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    """Create one reusable Kafka producer for the API ingest path."""
    for attempt in range(1, retries + 1):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
                acks=1,
                linger_ms=20,
                batch_size=131072,
                compression_type="gzip",
                retries=5,
            )
            print(f"[OK] Kafka producer connected (attempt {attempt})")
            return kafka_producer
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka not ready, retrying in {delay}s... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka for API ingestion.")


def chunk_logs(logs: list[str], chunk_size: int) -> list[list[str]]:
    return [logs[index : index + chunk_size] for index in range(0, len(logs), chunk_size)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, redis_client
    producer = create_producer()
    redis_client = create_redis_client(REDIS_URL)
    yield
    if producer is not None:
        producer.flush()
        producer.close()
    if redis_client is not None:
        redis_client.close()


app = FastAPI(lifespan=lifespan)


@app.post("/ingest", status_code=202)
async def ingest_logs(batch: LogBatch):
    """Accept logs quickly and hand them off to Kafka for async processing."""
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not ready.")

    try:
        for log_chunk in chunk_logs(batch.logs, KAFKA_BATCH_SIZE):
            producer.send(
                KAFKA_TOPIC,
                {
                    "logs": log_chunk,
                    "received_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "source": "http-ingest",
                },
            )
    except KafkaError as exc:
        raise HTTPException(status_code=503, detail="Failed to enqueue logs in Kafka.") from exc

    return {"message": f"Accepted {len(batch.logs)} logs for async processing"}


@app.get("/status", response_model=WindowReport)
async def get_status():
    """Return the latest window status produced by the async consumer."""
    if redis_client is None:
        return WindowReport(
            window_id=0,
            window_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_requests=0,
            status="WAITING",
        )

    latest_report = load_latest_report(redis_client)
    if latest_report is None:
        return WindowReport(
            window_id=0,
            window_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_requests=0,
            status="WAITING",
        )
    return latest_report
