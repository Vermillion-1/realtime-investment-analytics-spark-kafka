import time
import json
import os
import logging
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import sys

# Configuration
KAFKA_TOPIC = "yelp-reviews"
KAFKA_SERVER = "localhost:9092"
DATA_PATH = os.getenv("S3_INPUT_PATH", "s3://YOUR-BUCKET/yelp_parquet/Nashville/review.parquet")
DELAY_SECONDS = 0.05

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import s3fs
except ImportError:
    logger.error("s3fs is required to read from S3. Please install it: pip install s3fs")
    sys.exit(1)

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Connected to Kafka at {KAFKA_SERVER}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return None

def stream_data(producer):
    logger.info(f"Reading data from {DATA_PATH}...")
    try:
        df = pd.read_parquet(DATA_PATH)
        
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
            df = df.sort_values('date')
            
        records = df.to_dict(orient='records')
        logger.info(f"Loaded {len(records)} records. Starting stream from {records[0]['date']}...")
    
        count = 0
        for record in records:
            try:
                record['stream_timestamp'] = datetime.utcnow().isoformat()
                producer.send(KAFKA_TOPIC, value=record)
                count += 1
                
                if count % 100 == 0:
                    logger.info(f"Sent {count} records...")
                    
                time.sleep(DELAY_SECONDS)
                
            except KeyboardInterrupt:
                logger.info("Stopping stream...")
                break
            except Exception as e:
                logger.error(f"Error sending record: {e}")
    
        producer.flush()
        logger.info(f"Finished streaming {count} records.")
        
    except Exception as e:
        logger.error(f"Failed to read data from {DATA_PATH}: {e}")

if __name__ == "__main__":
    producer = create_producer()
    if producer:
        stream_data(producer)
        producer.close()