"""Redis-backed store for the latest analysis report."""

import logging
import time

from redis import Redis
from redis.exceptions import RedisError

from app.schemas import WindowReport

log = logging.getLogger(__name__)

LATEST_REPORT_KEY = "aiops:latest_report"


def create_redis_client(redis_url: str, retries: int = 10, delay: int = 2) -> Redis:
    """Connect to Redis with retries so Compose startup order is less fragile."""
    for attempt in range(1, retries + 1):
        try:
            client = Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            log.info("Redis connected (attempt %d)", attempt)
            return client
        except RedisError:
            log.warning("Redis not ready, retrying in %ds... (%d/%d)", delay, attempt, retries)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Redis.")


def save_report(client: Redis, report: WindowReport) -> None:
    """Persist the latest window report to Redis."""
    client.set(LATEST_REPORT_KEY, report.model_dump_json())


def load_report(client: Redis) -> WindowReport | None:
    """Load the latest window report from Redis, or None if nothing stored yet."""
    payload = client.get(LATEST_REPORT_KEY)
    if not payload:
        return None
    return WindowReport.model_validate_json(payload)
