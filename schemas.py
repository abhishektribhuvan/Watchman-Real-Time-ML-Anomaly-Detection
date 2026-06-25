# Pydantic models for request validation

from typing import List

from pydantic import BaseModel, Field


class LogBatch(BaseModel):
    """Payload for the /ingest endpoint."""

    logs: List[str]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "logs": [
                        "127.0.0.1 - - [24/Jun/2026:10:00:00 +0000] \"GET /api/v1/users HTTP/1.1\" 200 1024 15",
                        "127.0.0.1 - - [24/Jun/2026:10:00:01 +0000] \"POST /api/v1/payment HTTP/1.1\" 500 256 1025",
                        "127.0.0.1 - - [24/Jun/2026:10:00:02 +0000] \"GET /status HTTP/1.1\" 200 512 5"
                    ]
                }
            ]
        }
    }


class WindowReport(BaseModel):
    """The 5-second window analysis report returned by GET /status."""

    window_id: int = Field(..., description="Incrementing window counter")
    window_time: str = Field(..., description="When this window was analyzed")
    total_requests: int = Field(default=0, description="Logs received in this 5-sec window")
    error_count: int = Field(default=0, description="Number of 5xx responses")
    error_rate: float = Field(default=0.0, description="Percentage of 5xx errors (0.0 to 1.0)")
    avg_latency_ms: float = Field(default=0.0, description="Average response time")
    p99_latency_ms: float = Field(default=0.0, description="99th percentile response time")
    max_latency_ms: int = Field(default=0, description="Slowest request")
    status: str = Field(default="HEALTHY", description="HEALTHY, WARNING, CRITICAL, IDLE, or WAITING")
    is_anomaly: bool = Field(default=False)
    confidence: float = Field(default=0.0, description="0.0 to 1.0")
    anomaly_reasons: List[str] = Field(default_factory=list, description="What triggered the anomaly")
