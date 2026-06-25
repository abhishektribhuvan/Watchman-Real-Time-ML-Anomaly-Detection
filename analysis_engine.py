from datetime import datetime

import numpy as np

from schemas import WindowReport
from utils import parse_log_line


def analyze_window(logs_window: list[str], model, window_id: int) -> WindowReport:
    """Analyze one window of logs and return a status report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_logs = len(logs_window)

    if total_logs == 0:
        return WindowReport(
            window_id=window_id,
            window_time=now,
            total_requests=0,
            status="IDLE",
        )

    error_5xx, error_4xx = 0, 0
    latencies: list[int] = []

    for line in logs_window:
        status, latency = parse_log_line(line)
        if status is None:
            continue

        if 500 <= status < 600:
            error_5xx += 1
        elif 400 <= status < 500:
            error_4xx += 1
        latencies.append(latency)

    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    sorted_latencies = sorted(latencies)
    p99_index = max(int(len(sorted_latencies) * 0.99) - 1, 0)
    p99_latency = sorted_latencies[p99_index] if sorted_latencies else 0
    max_latency = max(latencies) if latencies else 0
    error_rate = error_5xx / total_logs if total_logs > 0 else 0.0

    vector = np.array([[total_logs, error_5xx, error_4xx, avg_latency]])
    prediction = model.predict(vector)[0]
    score = model.score_samples(vector)[0]

    confidence = min(max((-score - 0.3) / 0.3, 0.0), 1.0)
    is_anomaly = prediction == -1

    if is_anomaly and error_rate > 0.3:
        status_label = "CRITICAL"
    elif is_anomaly:
        status_label = "WARNING"
    else:
        status_label = "HEALTHY"

    anomaly_reasons = []
    if is_anomaly:
        if error_rate > 0.1:
            anomaly_reasons.append(f"High 5xx error rate: {error_rate:.1%}")
        if avg_latency > 3000:
            anomaly_reasons.append(f"High avg latency: {avg_latency:.0f}ms")
        if total_logs > 5000:
            anomaly_reasons.append(f"Traffic spike: {total_logs} requests in 5s")
        if not anomaly_reasons:
            anomaly_reasons.append(f"Anomalous pattern detected (score: {score:.3f})")

    return WindowReport(
        window_id=window_id,
        window_time=now,
        total_requests=total_logs,
        error_count=error_5xx,
        error_rate=round(error_rate, 4),
        avg_latency_ms=round(avg_latency, 1),
        p99_latency_ms=round(p99_latency, 1),
        max_latency_ms=max_latency,
        status=status_label,
        is_anomaly=is_anomaly,
        confidence=round(confidence, 3),
        anomaly_reasons=anomaly_reasons,
    )
