from pydantic import BaseModel, Field


class LogBatch(BaseModel):
    logs: list[str]


class WindowReport(BaseModel):
    window_id: int = Field(...)
    window_time: str = Field(...)
    total_requests: int = Field(default=0)
    error_count: int = Field(default=0)
    error_rate: float = Field(default=0.0)
    avg_latency_ms: float = Field(default=0.0)
    p99_latency_ms: float = Field(default=0.0)
    status: str = Field(default="HEALTHY")
    is_anomaly: bool = Field(default=False)
    anomaly_reasons: list[str] = Field(default_factory=list)
