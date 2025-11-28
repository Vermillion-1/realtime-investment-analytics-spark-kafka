# Nashville Tourism & Investment Dashboard: Project Overview

## The Core Question
**"Where are the untapped investment opportunities in Nashville's tourism market?"**

Investors and property managers often flock to the most popular, crowded areas (like Downtown Broadway). However, this project asks: **Are there neighborhoods where tourism demand (Yelp sentiment/activity) is high, but the supply of high-quality accommodation (Airbnb) is lagging?**

We call these areas **"Hidden Gems"**—zones where you can potentially earn higher revenue because the market isn't saturated yet.

---

## The Analysis: How We Find "Hidden Gems"

We combine two massive datasets to answer this question:
1.  **Demand (Yelp)**: What are tourists *doing* and *feeling*? (Reviews of restaurants, bars, attractions).
2.  **Supply (Airbnb)**: Where are they *staying* and how much are they paying? (Listings, prices, reviews).

### 1. Real-Time Sentiment Pulse (The "Vibe" Check)
We don't just look at star ratings. We stream live Yelp reviews and use **VADER Sentiment Analysis** to understand the *emotional* tone of the city in real-time.
*   **Why?** A 4-star review might say "Good food but terrible service." VADER captures that nuance.
*   **The "Tourism Gap"**: We compare the sentiment of *places to go* (Yelp) vs. *places to stay* (Airbnb).
    *   **"Great Food, Poor Stay"**: If Yelp sentiment is high but Airbnb sentiment is low, it means tourists love visiting but hate their hotels/rentals. **This is a prime opportunity to build better accommodation.**

### 2. Machine Learning for Investment Prediction
We use **Linear Regression** to predict the *expected* revenue of an Airbnb based on its location's commercial activity (nearby businesses, check-ins, ratings).
*   **The Logic**: Revenue should theoretically correlate with how busy and popular the neighborhood is.
*   **The "Arbitrage"**:
    *   If an area's *actual* revenue is **lower** than what our model predicts it *should* be, it might be under-managed or undervalued.
    *   If an area's *actual* revenue is **higher** than predicted, it's a "Hidden Gem"—it's outperforming expectations, likely due to high demand and low supply.

---

## Seasonal Trends
Yes! The project is designed to handle seasonality in two ways:
1.  **Streaming Window Aggregation**: The dashboard calculates sentiment over a **30-day sliding window**. This allows us to see how the "mood" of the city shifts from month to month (e.g., during the Country Music Awards vs. a quiet winter month).
2.  **Historical Trend Line**: The dashboard plots the "Sentiment Gap" over time, allowing stakeholders to identify if specific seasons (like Summer festivals) lead to a spike in complaints about accommodation (indicating a seasonal shortage of good stays).

---

## Technical Summary (Under the Hood)
*   **Ingestion**: We simulate a live feed of Yelp reviews using **Kafka**.
*   **Processing**: **Spark Structured Streaming** reads this live feed and joins it with a massive static dataset of Airbnb listings (stored in **S3**).
*   **Machine Learning**: We use **Spark ML** to cluster the city into 35 distinct "micro-zones" (using K-Means) and then run regression analysis on each zone.
*   **Visualization**: A **Streamlit** dashboard brings it all together, showing live metrics and a map of investment zones.
