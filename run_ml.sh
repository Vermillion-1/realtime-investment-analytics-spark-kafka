#!/bin/bash

# Load Environment Variables (for AWS Keys)
source ~/.bashrc

echo "Starting ML Training (Linear Regression)..."

# Run Spark Submit with S3 Dependencies
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.638 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  Linear_Regression_Nashville.py

echo "Training Complete! Output saved to /home/ubuntu/nashville_investment_map_output"
