"""Pydantic models for API request/response validation."""

from pydantic import BaseModel, Field


class LogBatch(BaseModel):
    """Payload for the /ingest endpoint."""

    logs: list[str]


class WindowReport(BaseModel):
    """Analysis report for a 5-second window of logs."""

    window_id: int = Field(..., description="Incrementing window counter")
    window_time: str = Field(..., description="Timestamp of analysis")
    total_requests: int = Field(default=0)
    error_count: int = Field(default=0, description="Number of 5xx responses")
    error_rate: float = Field(default=0.0, description="Ratio of 5xx errors")
    avg_latency_ms: float = Field(default=0.0)
    p99_latency_ms: float = Field(default=0.0)
    status: str = Field(default="HEALTHY", description="HEALTHY | WARNING | CRITICAL | IDLE | WAITING")
    is_anomaly: bool = Field(default=False)
    anomaly_reasons: list[str] = Field(default_factory=list)
