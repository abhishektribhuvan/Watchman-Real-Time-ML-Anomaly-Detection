"""ML analysis engine — scores a window of logs for anomalies."""

from datetime import datetime

import numpy as np

from app.schemas import WindowReport
from app.utils import parse_log_line


def analyze_window(logs: list[str], model, window_id: int) -> WindowReport:
    """Analyze one 5-second window of logs and return a health report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not logs:
        return WindowReport(window_id=window_id, window_time=now, status="IDLE")

    # Parse all logs and extract metrics
    error_5xx = 0
    error_4xx = 0
    latencies: list[int] = []

    for line in logs:
        status, latency = parse_log_line(line)
        if status is None:
            continue
        if 500 <= status < 600:
            error_5xx += 1
        elif 400 <= status < 500:
            error_4xx += 1
        latencies.append(latency)

    total = len(logs)
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    error_rate = error_5xx / total if total > 0 else 0.0

    # P99 latency
    sorted_lat = sorted(latencies)
    p99 = sorted_lat[max(int(len(sorted_lat) * 0.99) - 1, 0)] if sorted_lat else 0

    # ML prediction
    features = np.array([[total, error_5xx, error_4xx, avg_latency]])
    prediction = model.predict(features)[0]
    is_anomaly = prediction == -1

    # Determine status
    if is_anomaly and error_rate > 0.3:
        status_label = "CRITICAL"
    elif is_anomaly:
        status_label = "WARNING"
    else:
        status_label = "HEALTHY"

    # Build human-readable reasons (only when anomaly detected)
    reasons = []
    if is_anomaly:
        if error_rate > 0.1:
            reasons.append(f"High error rate ({error_rate:.0%})")
        if avg_latency > 3000:
            reasons.append(f"Slow responses ({avg_latency:.0f}ms avg)")
        if total > 5000:
            reasons.append(f"Traffic spike ({total} reqs)")
        if not reasons:
            reasons.append("Unusual pattern detected by model")

    return WindowReport(
        window_id=window_id,
        window_time=now,
        total_requests=total,
        error_count=error_5xx,
        error_rate=round(error_rate, 4),
        avg_latency_ms=round(avg_latency, 1),
        p99_latency_ms=round(p99, 1),
        status=status_label,
        is_anomaly=is_anomaly,
        anomaly_reasons=reasons,
    )
