# AIOps RCA Engine

ML-powered Root Cause Analysis engine for server. Ingests server logs in real-time, detects anomalies using an Isolation Forest ML model, and reports system health via a REST API.

**Key Capabilities:**
- **High Throughput:** Designed to handle traffic spikes of up to **500 logs per second** concurrently without dropping requests for current free tier servers , increasing the ram can handle 10x more requests per sec .   
- **Batch Ingestion:** The `/ingest` endpoint accepts payloads of up to **1,000 logs per batch**, reducing network overhead.
- **Asynchronous Processing:** Uses Apache Kafka to decouple ingestion from machine learning analysis, ensuring the API consistently responds in under **20ms**.
- **Anomaly Detection:** Utilizes Scikit-Learn's Isolation Forest to identify unusual traffic patterns based on error rates and latency in **5-second rolling windows**.

---

## API Demo

You can test the engine locally or live on an AWS EC2 instance.

**API Documentation (Swagger UI):** `http://51.21.244.122:8000/docs` *(Replace with localhost or your actual IP)*

### How to test the ML Engine:
1. Open the Live API Documentation link above in your browser.
2. Click on the green `POST /ingest` row to expand it, then click the **"Try it out"** button.
3. Paste the following example log data (which includes some simulated errors and latency) into the **Request body** field:
```json
{
  "logs": [
    "148.5.169.251 - - [27/Dec/2037:12:00:00 +0530] \"DELETE /usr/admin HTTP/1.0\" 200 5044 \"http://app.com\" \"Mozilla/5.0\" 1409",
    "90.64.62.239 - - [27/Dec/2037:12:00:00 +0530] \"PUT /usr/admin/developer HTTP/1.0\" 500 4947 \"http://app.com\" \"Mozilla/5.0\" 1127",
    "97.15.37.214 - - [27/Dec/2037:12:00:00 +0530] \"PUT /usr/login HTTP/1.0\" 404 4980 \"-\" \"Mozilla/5.0\" 2258",
    "159.238.93.133 - - [27/Dec/2037:12:00:00 +0530] \"POST /usr/register HTTP/1.0\" 200 4959 \"http://app.com\" \"Mozilla/5.0\" 4871",
    "238.204.144.175 - - [27/Dec/2037:12:00:00 +0530] \"POST /usr/login HTTP/1.0\" 500 5056 \"http://app.com\" \"Mozilla/5.0\" 455",
    "66.181.188.94 - - [27/Dec/2037:12:00:00 +0530] \"POST /usr HTTP/1.0\" 200 4966 \"-\" \"Mozilla/5.0\" 3609"
  ]
}
```
4. Click the blue **Execute** button. You should instantly get a `202 Accepted` response.
5. Wait about **5 to 6 seconds**. (The engine processes logs in 5-second asynchronous windows).
6. Scroll down to the blue `GET /status` row, click **"Try it out"**, and then **Execute**. 
7. You will see the ML analysis results! It will show the total requests processed, error rates, average latency, and whether the Machine Learning model flagged this window as an anomaly.

---

## Architecture

```
Logs ──POST──▶ FastAPI /ingest ──▶ Kafka ──▶ ML Consumer ──▶ Redis
                                                                │
                              GET /status ◀─────────────────────┘
```

1. **Ingest** — Applications POST logs to `/ingest`. FastAPI hands them to Kafka instantly (HTTP 202).
2. **Queue** — Kafka buffers logs for reliable async processing.
3. **Analyze** — A background consumer reads logs in 5-second windows, runs them through the ML model, and detects anomalies.
4. **Report** — Results are saved to Redis. Hit `/status` anytime for the latest health report.

## Project Structure

```
aiops_rca_engine/
├── app/
│   ├── api.py              # FastAPI server (/ingest + /status)
│   ├── schemas.py          # Pydantic request/response models
│   ├── status_store.py     # Redis read/write operations
│   └── utils.py            # Log parsing utilities
├── ml/
│   ├── analysis_engine.py  # ML anomaly detection logic
│   ├── train_model.py      # Model training script
│   └── model.pkl           # Pre-trained Isolation Forest model
├── streaming/
│   └── kafka_consumer_ml.py  # Kafka consumer + ML pipeline
├── Dockerfile
├── docker-compose.yml      # All services with memory limits
├── deploy.sh               # One-command EC2 deployment
└── requirements.txt
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API | FastAPI + Uvicorn |
| Message Queue | Apache Kafka + Zookeeper |
| Cache | Redis |
| ML Model | Scikit-Learn (Isolation Forest) |
| Deployment | Docker Compose on AWS EC2 |

---

## Quick Start (Local)

```bash
docker-compose up -d --build
```

Check status:
```bash
curl http://localhost:8000/status
```

Send test logs:
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"logs": ["127.0.0.1 - - [26/Jun/2026:10:00:00 +0000] \"GET /api HTTP/1.1\" 200 1024 \"http://example.com\" \"Mozilla/5.0\" 15"]}'
```

Check status again:
```bash
curl http://localhost:8000/status
```

API docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Deploy to AWS EC2

### Prerequisites
- An EC2 instance (Ubuntu, t2.micro or larger)
- Security group with ports **8000**, **22** open
- Your `.pem` key file

### One-Command Deploy

```bash
chmod +x deploy.sh
./deploy.sh <EC2_PUBLIC_IP> <PATH_TO_PEM_FILE>
```

Example:
```bash
./deploy.sh 54.123.45.67 ~/keys/my-key.pem
```

This will:
1. Install Docker on the EC2 instance (if needed)
2. Upload all project files
3. Build and start all services
4. Verify the deployment

### Manual Deploy (Step by Step)

```bash
# 1. SSH into your EC2 instance
ssh -i your-key.pem ubuntu@<EC2_IP>

# 2. Install Docker
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2
sudo usermod -aG docker ubuntu
sudo systemctl enable docker && sudo systemctl start docker

# 3. Upload project (from your local machine)
scp -i your-key.pem -r app ml streaming requirements.txt Dockerfile docker-compose.yml ubuntu@<EC2_IP>:~/aiops_rca_engine/

# 4. Start everything
cd ~/aiops_rca_engine
export EC2_PUBLIC_IP=<EC2_IP>
sudo EC2_PUBLIC_IP=$EC2_PUBLIC_IP docker compose up -d --build

# 5. Verify
curl http://localhost:8000/status
```

### Memory Usage (t2.micro — 1 GB RAM)

| Service | Memory Limit |
|---------|-------------|
| Zookeeper | 200 MB |
| Kafka | 350 MB |
| Redis | 50 MB |
| API | 150 MB |
| ML Consumer | 150 MB |
| **Total** | **~900 MB** |

### Useful Commands (on EC2)

```bash
# View all running containers
sudo docker compose ps

# Check memory usage
sudo docker stats --no-stream

# View logs
sudo docker compose logs -f

# Restart everything
sudo docker compose restart

# Stop everything
sudo docker compose down
```

---

## Retraining the Model

If you have new `server.log` data:

```bash
python -m ml.train_model
```

This reads the first 10,000 lines of `server.log`, trains an Isolation Forest, and saves `model.pkl`.
