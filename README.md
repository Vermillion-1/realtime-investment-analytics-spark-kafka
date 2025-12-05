# Nashville Tourism & Investment Dashboard

## Project Overview

### The Core Question
**"Where are the untapped investment opportunities in Nashville's tourism market?"**

Investors often flock to crowded areas like Broadway. This project asks: **Are there neighborhoods where tourism demand (Yelp sentiment) is high, but the supply of high-quality accommodation (Airbnb) is lagging?** We call these **"Hidden Gems"**.

### The Analysis
We combine two massive datasets:
1.  **Demand (Yelp)**: Real-time reviews to gauge what tourists *feel* (using **VADER Sentiment Analysis**).
2.  **Supply (Airbnb)**: Listings data to see where they *stay* and the revenue potential.

**The Strategy:**
*   **Sentiment Gap**: High Yelp Sentiment + Low Airbnb Sentiment = Opportunity.
*   **Machine Learning**: We use **Linear Regression** to predict expected revenue. If a zone earns *more* than predicted, it's a "Hidden Gem".
*   **Seasonality**: We track sentiment over a 30-day sliding window to capture seasonal shifts (e.g., festivals vs. winter).

### Technical Architecture
*   **Ingestion**: Kafka streams live Yelp reviews.
*   **Processing**: Spark Structured Streaming joins live Yelp data with static Airbnb data (S3).
*   **ML**: Spark ML (Linear Regression) clusters the city into 35 zones and predicts revenue.
*   **Viz**: Streamlit Dashboard for real-time monitoring.

---

## Project Structure

The repository is organized into the following logical components:

### 1. Data Ingestion & Streaming
*   `yelp_producer.py`: **Kafka Producer**. Reads Yelp Parquet data and streams it to the `yelp-reviews` topic.
*   `sentiment_streaming.py`: **Spark Streaming Job**. Consumes Kafka stream, joins with Airbnb data, applies VADER sentiment analysis, and aggregates metrics.

### 2. Machine Learning
*   `Linear_Regression_Nashville.py`: **ML Training Script**. Loads data, clusters Nashville into 35 zones (K-Means), and trains the Linear Regression model to identify investment zones.
*   `Nashville_Investment_GBT.py`: (Alternative) Gradient Boosted Tree model implementation.

### 3. Visualization
*   `dashboard.py`: **Streamlit App**. The frontend dashboard that visualizes the real-time "Pulse" and the "Investment Map".

### 4. ETL & Utilities
*   `split_yelp_*.py`: **ETL Scripts**. A suite of scripts used to pre-process the massive raw Yelp JSON dataset into optimized Parquet files partitioned by city.
*   `docker-compose.yml`: Configuration for the Kafka and Zookeeper services.

### 5. Automation Scripts
*   `start_pipeline.sh`: **Master Script**. Launches Kafka, the Producer, and the Spark Streaming job in one go.
*   `run_ml.sh`: **ML Helper**. Submits the ML training job to Spark with the necessary S3 dependencies.
*   `setup_ec2.sh`: **Infrastructure**. Installs Docker, Java, Spark, and Python dependencies on a fresh EC2 instance.

---

## Quick Start

### 1. Data Setup
**Note:** Large datasets are hosted externally.
1.  Download `yelp_parquet/` and `Airbnb_by_city/` from [Google Drive Link].
2.  Place them in the project root.

### 2. Run the Pipeline
**Terminal 1: Start Real-Time System**
```bash
./start_pipeline.sh
```
*Starts Kafka, Producer, and Spark Streaming.*

**Terminal 2: Generate Intelligence & View**
```bash
./run_ml.sh               # Trains model & finds Hidden Gems
streamlit run dashboard.py # Launches Dashboard
```
*Access at `http://localhost:8501` (or your EC2 IP).*
