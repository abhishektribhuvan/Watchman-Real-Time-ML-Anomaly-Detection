import os
import random
import time
from itertools import islice

import requests

API_URL = os.environ.get("API_URL", "http://localhost:8000/ingest")


def simulate():
    print("Loading log file...")
    with open("server.log", "r") as file_handle:
        skipped = 0
        for _ in islice(file_handle, 10000):
            skipped += 1
        print(f"Skipped {skipped} training lines.")

        remaining_logs = [line.rstrip("\n") for line in file_handle]

    if not remaining_logs:
        print("[WARN] No logs remaining after skipping training data.")
        return

    print(f"Loaded {len(remaining_logs)} logs for simulation.")
    print("Starting traffic blast to API...")

    current_index = 0
    total_logs = len(remaining_logs)

    while current_index < total_logs:
        if random.random() > 0.9:
            traffic_volume = random.randint(5000, 10000)
        else:
            traffic_volume = random.randint(10, 500)

        batch = remaining_logs[current_index : current_index + traffic_volume]
        current_index += traffic_volume

        try:
            response = requests.post(API_URL, json={"logs": batch}, timeout=10)
            if response.status_code in (200, 202):
                print(f"[Simulator] Sent batch of {len(batch)} logs.")
            else:
                print(f"[Simulator] API returned status {response.status_code}")
        except requests.exceptions.ConnectionError:
            print("[Simulator] API is down or not responding. Retrying in 1s...")

        time.sleep(1)

    print("[OK] All logs sent. Simulation complete.")


if __name__ == "__main__":
    simulate()
