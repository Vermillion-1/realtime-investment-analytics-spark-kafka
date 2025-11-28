#!/bin/bash

# 1. Load Environment Variables
source ~/.bashrc

# 2. Start Kafka (if not running)
echo "Starting Kafka..."
sudo docker-compose up -d
sleep 5

# 3. Start Producer
echo "Starting Producer..."
pkill -f yelp_producer.py # Kill existing to avoid duplicates
nohup python3 yelp_producer.py > producer.log 2>&1 &
echo "Producer logs: tail -f producer.log"

# 4. Start Consumer
echo "Starting Consumer..."
# Clean checkpoint for fresh start (Optional, comment out for production)
# rm -rf checkpoint_sentiment 

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.638 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  sentiment_streaming.py
