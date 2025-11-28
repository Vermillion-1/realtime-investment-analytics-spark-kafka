#!/bin/bash
set -e

echo "=========================================="
echo "   Starting EC2 Setup for Project    "
echo "=========================================="

# 1. Update System
echo "[1/5] Updating system packages..."
sudo apt-get update -y
sudo apt-get upgrade -y

# 2. Install Java (Required for Spark)
echo "[2/5] Installing Java (OpenJDK 17)..."
sudo apt-get install -y openjdk-17-jdk
java -version

# 3. Install Docker & Docker Compose
echo "[3/5] Installing Docker..."
sudo apt-get install -y docker.io docker-compose
sudo usermod -aG docker $USER
echo "Docker installed. Note: You may need to re-login for group changes to take effect."

# 4. Install Python & Libraries
echo "[4/5] Installing Python dependencies..."
sudo apt-get install -y python3-pip
pip3 install pyspark kafka-python pandas vaderSentiment pyarrow
# Install AWS CLI (useful for debugging S3)
sudo apt-get install -y awscli

# 5. Final Checks
echo "=========================================="
echo "   Setup Complete!                        "
echo "=========================================="
echo "Versions:"
echo "Java: $(java -version 2>&1 | head -n 1)"
echo "Docker: $(docker --version)"
echo "Python: $(python3 --version)"
echo "=========================================="
echo "NEXT STEPS:"
echo "1. Logout and log back in: 'exit' then 'ssh ...'"
echo "2. Upload your code and data."
echo "3. Run 'docker-compose up -d' to start Kafka."
