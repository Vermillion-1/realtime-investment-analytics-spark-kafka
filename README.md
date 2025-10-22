# CMPT-732-Big-Data-1-Project
# Yelp Dataset Processing

Scripts for processing the Yelp Open Dataset into city-separated JSON files and Parquet format for Spark analytics.

## Overview

This repository contains tools to:
1. **Split the Yelp dataset by city** - Separate the monolithic JSON files into individual city directories
2. **Convert to Parquet format** - Transform JSON data into columnar Parquet files optimized for Apache Spark

## Dataset Structure

### Source Data
- **Location**: `Yelp JSON/yelp_dataset.tar`
- **Size**: ~4.5 GB compressed, ~8 GB extracted
- **Files**:
  - `yelp_academic_dataset_business.json` - Business information
  - `yelp_academic_dataset_review.json` - User reviews
  - `yelp_academic_dataset_user.json` - User profiles
  - `yelp_academic_dataset_tip.json` - Quick tips
  - `yelp_academic_dataset_checkin.json` - Check-in timestamps
  - `yelp_academic_dataset_photo.json` - Photo metadata

### Processed Data

#### `yelp_by_city/` - City-Separated JSON
```
yelp_by_city/
├── Toronto/
│   ├── business.json
│   ├── review.json
│   ├── user.json
│   ├── tip.json
│   ├── checkin.json
│   └── photo.json
├── Montreal/
├── Vancouver/
└── ...
```

#### `yelp_parquet/` - Parquet Format
```
yelp_parquet/
├── Toronto/
│   ├── business.parquet/
│   │   └── state=ON/
│   ├── review.parquet/
│   │   ├── year=2020/
│   │   ├── year=2021/
│   │   └── ...
│   ├── user.parquet/
│   └── ...
├── Montreal/
└── ...
```
- **Format**: Apache Parquet (columnar storage)
- **Partitioning**: 
  - Business: by `state`
  - Review: by `year`
  - Photo: by `label`
- **Benefits**: 50-80% smaller, faster Spark queries

## Scripts

### 1. `split_yelp_by_city.py`

Splits the Yelp dataset into separate directories per city.

**Usage:**
```bash
python split_yelp_by_city.py
```

**Configuration** (edit `main()` function):
```python
# Canadian cities only
CANADIAN_ONLY = True
SPECIFIC_CITIES = []

# All cities
CANADIAN_ONLY = False
SPECIFIC_CITIES = []

# Specific cities only
CANADIAN_ONLY = False
SPECIFIC_CITIES = ['Toronto', 'Montreal', 'Vancouver']
```

### 2. `split_yelp_by_city_parquet.py`

Converts city-separated JSON files to Parquet format using Apache Spark.

**Requirements:**
```bash
pip install pyspark
```

**Usage:**
```bash
python convert_to_parquet.py
```

**Configuration** (edit `main()` function):
```python
# Canadian cities only
CANADIAN_ONLY = True

# All cities
CANADIAN_ONLY = False
```

**Features:**
- Uses Apache Spark for efficient processing
- Smart partitioning for query optimization
- Automatic schema inference
- Validates Canadian provinces before processing

**Spark Configuration:**
- Driver memory: 4GB
- Shuffle partitions: 10

## Dataset Information

### Geographic Coverage
The Yelp Open Dataset includes:
- **United States**: Phoenix, Las Vegas, Tampa, Indianapolis, Nashville, New Orleans, Reno, Tucson, Santa Barbara
- **Canada**: Toronto, Montreal, Calgary, Vancouver, Edmonton, Mississauga, Ottawa
- **Europe**: Limited coverage

### Canadian Province Codes
AB, BC, MB, NB, NL, NS, NT, NU, ON, PE, QC, SK, YT

### Data Statistics (Example)
```
Top 10 Cities by Business Count:
  Philadelphia: 15,500 businesses
  Tampa: 13,400 businesses
  Indianapolis: 9,800 businesses
  ...
```

## Using with Apache Spark

### Load Parquet Data
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Yelp Analysis") \
    .getOrCreate()

# Load specific city
df = spark.read.parquet('yelp_parquet/Toronto/business.parquet')

# Load all cities
df = spark.read.parquet('yelp_parquet/*/business.parquet')

# Leverage partitioning
reviews = spark.read.parquet('yelp_parquet/*/review.parquet') \
    .filter("year >= 2020")  # Only reads 2020+ partitions
```

### Query Examples
```python
# Top rated businesses in Toronto
top_rated = spark.read.parquet('yelp_parquet/Toronto/business.parquet') \
    .filter("stars >= 4.5") \
    .orderBy("review_count", ascending=False) \
    .limit(10)

# Review sentiment by year
sentiment = spark.read.parquet('yelp_parquet/*/review.parquet') \
    .groupBy("year") \
    .agg({"stars": "avg"})
```

## Important Notes

### User Duplication
Users who reviewed businesses in multiple cities appear in each city's `user.json`. This is by design to keep each city's dataset self-contained.



## Schema Reference

### Business
```
business_id: string
name: string
address: string
city: string
state: string
postal_code: string
latitude: float
longitude: float
stars: float
review_count: int
is_open: int
attributes: struct
categories: array<string>
hours: struct
```

### Review
```
review_id: string
user_id: string
business_id: string
stars: int
date: string
text: string
useful: int
funny: int
cool: int
```

### User
```
user_id: string
name: string
review_count: int
yelping_since: string
friends: array<string>
useful: int
funny: int
cool: int
fans: int
elite: array<int>
average_stars: float
compliment_*: int (various types)
```

## Resources

- [Yelp Open Dataset](https://www.yelp.com/dataset)
- [Yelp Dataset Documentation](https://www.yelp.com/dataset/documentation/main)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Parquet Format](https://parquet.apache.org/)

## License

The Yelp Dataset is provided under the [Yelp Dataset License Agreement](https://s3-media2.fl.yelpcdn.com/assets/srv0/engineering_pages/bea5c1e92bf3/assets/vendor/yelp-dataset-agreement.pdf) for academic use only.

## 🤝 Contributing

For issues or improvements:
1. Check existing issues
2. Create a new issue describing the problem/enhancement
3. Submit a pull request with clear description

---

**Project**: CMPT732 Data Analytics Project  
**Institution**: Simon Fraser University  
**Last Updated**: October 2025
