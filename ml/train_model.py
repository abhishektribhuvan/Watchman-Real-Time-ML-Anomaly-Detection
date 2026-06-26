import numpy as np
import joblib
from itertools import islice
from sklearn.ensemble import IsolationForest
from app.utils import parse_log_line


def train():
    print("Reading logs for training...")

    # Read up to the first 10k lines safely (no crash if file is shorter)
    with open("server.log", "r") as f:
        logs = list(islice(f, 10000))

    if len(logs) < 100:
        print(f"[WARN] Only {len(logs)} logs found. Need at least 100 for meaningful training.")
        return

    print(f"Read {len(logs)} logs for training.")

    # We will simulate grouping these logs into 5-second windows.
    # Let's say we process 500 logs per window just to build training data.
    window_size = 500
    training_data = []

    for i in range(0, len(logs), window_size):
        batch = logs[i:i+window_size]
        
        total_logs = len(batch)
        error_5xx = 0
        error_4xx = 0
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
        
        # Feature Vector: [Volume, 5xx Count, 4xx Count, Avg Latency]
        training_data.append([total_logs, error_5xx, error_4xx, avg_latency])

    X = np.array(training_data)
    
    print(f"Extracted {len(training_data)} training vectors. Training Isolation Forest...")
    
    # Train the model. contamination=0.05 means we assume 5% of training data might be anomalies
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)
    
    # Save the trained model
    joblib.dump(model, "model.pkl")
    print("[OK] Model trained and saved as 'model.pkl'.")

if __name__ == "__main__":
    train()