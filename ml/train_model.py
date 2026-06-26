"""Train an Isolation Forest model on server logs for anomaly detection."""

import logging

import joblib
import numpy as np
from itertools import islice
from sklearn.ensemble import IsolationForest

from app.utils import parse_log_line

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("trainer")

WINDOW_SIZE = 500  # logs per training window


def train():
    """Read server.log, extract features in windows, train and save model."""
    log.info("Reading server.log for training data...")

    with open("server.log", "r") as f:
        lines = list(islice(f, 10_000))

    if len(lines) < 100:
        log.error("Only %d logs found — need at least 100 for training", len(lines))
        return

    log.info("Read %d log lines", len(lines))

    # Group logs into windows and extract feature vectors
    training_data = []
    for i in range(0, len(lines), WINDOW_SIZE):
        batch = lines[i : i + WINDOW_SIZE]

        error_5xx, error_4xx = 0, 0
        latencies = []

        for line in batch:
            status, latency = parse_log_line(line)
            if status is None:
                continue
            if 500 <= status < 600:
                error_5xx += 1
            elif 400 <= status < 500:
                error_4xx += 1
            latencies.append(latency)

        avg_latency = sum(latencies) / len(latencies) if latencies else 0

        # Feature vector: [volume, 5xx_count, 4xx_count, avg_latency]
        training_data.append([len(batch), error_5xx, error_4xx, avg_latency])

    X = np.array(training_data)
    log.info("Extracted %d training vectors — training Isolation Forest...", len(X))

    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)

    joblib.dump(model, "model.pkl")
    log.info("Model saved as model.pkl")


if __name__ == "__main__":
    train()