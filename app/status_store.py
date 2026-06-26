import time

from redis import Redis
from redis.exceptions import RedisError

from app.schemas import WindowReport

LATEST_REPORT_KEY = "aiops:latest_report"


def create_redis_client(redis_url: str, retries: int = 10, delay: int = 2) -> Redis:
    """Connect to Redis with retries so Compose startup is less fragile."""
    for attempt in range(1, retries + 1):
        try:
            client = Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            print(f"[OK] Redis connected (attempt {attempt})")
            return client
        except RedisError:
            print(f"[WAIT] Redis not ready, retrying in {delay}s... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Redis.")


def save_latest_report(redis_client: Redis, report: WindowReport) -> None:
    redis_client.set(LATEST_REPORT_KEY, report.model_dump_json())


def load_latest_report(redis_client: Redis) -> WindowReport | None:
    payload = redis_client.get(LATEST_REPORT_KEY)
    if not payload:
        return None
    return WindowReport.model_validate_json(payload)
