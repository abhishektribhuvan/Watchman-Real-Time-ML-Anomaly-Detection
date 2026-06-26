#!/bin/bash
# ===========================================================================
# deploy.sh — Deploy AIOps RCA Engine to an AWS EC2 instance
#
# Usage:
#   ./deploy.sh <EC2_PUBLIC_IP> <PATH_TO_PEM_FILE>
#
# Example:
#   ./deploy.sh 54.123.45.67 ~/keys/my-key.pem
# ===========================================================================

set -e

EC2_IP="$1"
PEM_FILE="$2"

if [ -z "$EC2_IP" ] || [ -z "$PEM_FILE" ]; then
    echo "Usage: ./deploy.sh <EC2_PUBLIC_IP> <PATH_TO_PEM_FILE>"
    exit 1
fi

SSH_CMD="ssh -i $PEM_FILE -o StrictHostKeyChecking=no ubuntu@$EC2_IP"
SCP_CMD="scp -i $PEM_FILE -o StrictHostKeyChecking=no"

echo ""
echo "=========================================="
echo "  AIOps RCA Engine — EC2 Deployment"
echo "=========================================="
echo "  Target: ubuntu@$EC2_IP"
echo ""

# -------------------------------------------------------------------
# Step 1: Install Docker & Docker Compose on EC2 (if not installed)
# -------------------------------------------------------------------
echo "[1/4] Installing Docker on EC2 (if needed)..."
$SSH_CMD << 'INSTALL_DOCKER'
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io docker-compose-v2
    sudo usermod -aG docker ubuntu
    sudo systemctl enable docker
    sudo systemctl start docker
    echo "Docker installed."
else
    echo "Docker already installed."
fi
INSTALL_DOCKER

# -------------------------------------------------------------------
# Step 2: Create project directory and upload files
# -------------------------------------------------------------------
echo "[2/4] Uploading project files to EC2..."
$SSH_CMD "mkdir -p ~/aiops_rca_engine"

# Upload only the files needed to run (not server.log, not .git)
$SCP_CMD -r \
    app \
    ml \
    streaming \
    requirements.txt \
    Dockerfile \
    docker-compose.yml \
    ubuntu@$EC2_IP:~/aiops_rca_engine/

echo "Files uploaded."

# -------------------------------------------------------------------
# Step 3: Build and start all services
# -------------------------------------------------------------------
echo "[3/4] Building and starting services on EC2..."
$SSH_CMD << DEPLOY
cd ~/aiops_rca_engine

# Set the public IP so Kafka advertises the correct address
export EC2_PUBLIC_IP=$EC2_IP

# Use newgrp to pick up docker group without re-login
sudo docker compose down 2>/dev/null || true
sudo EC2_PUBLIC_IP=$EC2_IP docker compose up -d --build

echo "Waiting for services to start..."
sleep 15
sudo docker compose ps
DEPLOY

# -------------------------------------------------------------------
# Step 4: Verify deployment
# -------------------------------------------------------------------
echo "[4/4] Verifying deployment..."
echo ""

RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://$EC2_IP:8000/status 2>/dev/null || echo "000")

if [ "$RESPONSE" = "200" ]; then
    echo "  Deployment successful!"
    echo ""
    echo "  Status:  http://$EC2_IP:8000/status"
    echo "  Ingest:  http://$EC2_IP:8000/ingest  (POST)"
    echo "  Docs:    http://$EC2_IP:8000/docs"
    echo ""
    echo "  Test with:"
    echo "    curl http://$EC2_IP:8000/status"
else
    echo "  API not responding yet (HTTP $RESPONSE)."
    echo "  Services may still be starting. Wait 30s and try:"
    echo "    curl http://$EC2_IP:8000/status"
    echo ""
    echo "  To check logs:"
    echo "    ssh -i $PEM_FILE ubuntu@$EC2_IP 'cd ~/aiops_rca_engine && sudo docker compose logs'"
fi

echo ""
echo "=========================================="
echo "  Deployment complete!"
echo "=========================================="
