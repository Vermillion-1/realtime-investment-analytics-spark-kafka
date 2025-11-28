# Nashville Tourism & Investment Dashboard

A Big Data pipeline combining real-time sentiment analysis (Yelp/Airbnb) with ML investment predictions.

## Data Setup
**Note:** Data is not included in this repo. Download it from [Google Drive Link] and place folders in the project root:
1.  `yelp_parquet/`
2.  `Airbnb_by_city/`

## Quick Start
**1. Setup Environment**
```bash
pip install pyspark kafka-python pandas vaderSentiment streamlit s3fs
sudo docker-compose up -d  # Start Kafka
```

**2. Run Pipeline (Terminal 1)**
```bash
# Streams Yelp data & calculates real-time sentiment
./start_pipeline.sh
```

**3. Run ML & Dashboard (Terminal 2)**
```bash
# Generates investment map
./run_ml.sh

# Launches visualization
streamlit run dashboard.py
```
*Access at `http://localhost:8501` (or EC2 IP)*

## Architecture
*   **Ingestion**: Kafka streams Yelp reviews.
*   **Processing**: Spark Structured Streaming joins live Yelp data with static Airbnb data.
*   **ML**: Linear Regression predicts "Hidden Gem" investment zones.
*   **Viz**: Streamlit Dashboard.

## Files
*   `yelp_producer.py`: Kafka Producer.
*   `sentiment_streaming.py`: Spark Streaming Job.
*   `Linear_Regression_Nashville.py`: ML Training.
*   `dashboard.py`: Dashboard App.
*   `start_pipeline.sh` / `run_ml.sh`: Automation Scripts.
