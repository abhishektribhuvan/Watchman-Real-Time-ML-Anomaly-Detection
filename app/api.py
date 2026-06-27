import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from app.schemas import LogBatch, WindowReport
from app.status_store import create_redis_client, load_report

log = logging.getLogger(__name__)

# config
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
KAFKA_BATCH_SIZE = int(os.environ.get("KAFKA_BATCH_SIZE", "500"))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

producer = None
redis_client = None


def _create_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            kp = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode(),
                acks=1,
                linger_ms=20,
                batch_size=131_072,
                compression_type="gzip",
                retries=5,
            )
            log.info("Kafka producer connected (attempt %d)", attempt)
            return kp
        except NoBrokersAvailable:
            log.warning("Kafka not ready — retrying in %ds (%d/%d)", delay, attempt, retries)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka.")


def _chunk(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global producer, redis_client
    producer = _create_producer()
    redis_client = create_redis_client(REDIS_URL)
    yield
    if producer:
        producer.flush()
        producer.close()
    if redis_client:
        redis_client.close()


app = FastAPI(title="AIOps RCA Engine", lifespan=lifespan)


@app.post("/ingest", status_code=202)
async def ingest_logs(batch: LogBatch):
    if producer is None:
        raise HTTPException(503, detail="Kafka producer not ready")

    try:
        for chunk in _chunk(batch.logs, KAFKA_BATCH_SIZE):
            producer.send(
                KAFKA_TOPIC,
                {
                    "logs": chunk,
                    "received_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "source": "http-ingest",
                },
            )
    except KafkaError as exc:
        raise HTTPException(503, detail="Failed to enqueue logs") from exc

    return {"accepted": len(batch.logs)}


@app.get("/status", response_model=WindowReport)
async def get_status():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if redis_client is None:
        return WindowReport(window_id=0, window_time=now, status="WAITING")

    report = load_report(redis_client)
    if report is None:
        return WindowReport(window_id=0, window_time=now, status="WAITING")

    return report
