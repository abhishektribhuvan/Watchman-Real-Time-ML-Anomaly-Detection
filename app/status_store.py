import logging
import time

from redis import Redis
from redis.exceptions import RedisError

from app.schemas import WindowReport

log = logging.getLogger(__name__)

LATEST_REPORT_KEY = "aiops:latest_report"


def create_redis_client(redis_url: str, retries: int = 10, delay: int = 2) -> Redis:
    for attempt in range(1, retries + 1):
        try:
            client = Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            return client
        except RedisError:
            time.sleep(delay)
    raise RuntimeError("Could not connect to Redis.")


def save_report(client: Redis, report: WindowReport) -> None:
    client.set(LATEST_REPORT_KEY, report.model_dump_json())


def load_report(client: Redis) -> WindowReport | None:
    payload = client.get(LATEST_REPORT_KEY)
    if not payload:
        return None
    return WindowReport.model_validate_json(payload)
