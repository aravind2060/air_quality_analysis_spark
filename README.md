# Air Quality Monitoring & Forecasting - Section 1

**Data Ingestion and Initial Pre-Processing**

---

## 📋 Objective

Ingest historical air quality data as simulated real-time streams, perform cleaning/merging, and produce a structured dataset combining air quality metrics with weather data.

---

## ⚙️ Environment Setup

### 1. Java Installation (Required for Spark)

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# Mac (Homebrew)
brew install openjdk@11
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk

# Verify
java -version  # Should show "11.x.x"

## 2. Python Setup (3.10)

# Create virtual environment
python3.10 -m venv aq-env
source aq-env/bin/activate  # Linux/Mac
# .\aq-env\Scripts\activate  # Windows

# Install dependencies
pip install pyspark==3.3.3 boto3==1.28.65

# Download Spark 3.3.3
wget https://archive.apache.org/dist/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz
tar xvf spark-3.3.3-bin-hadoop3.tgz
export SPARK_HOME=~/spark-3.3.3-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# Verify
spark-submit --version

```

### 🚀 Execution Pipeline

```bash
# Downloads and unzips OpenAQ data
python ingestion/download_from_s3.py

# Verify downloaded files
ls -l ingestion/data/pending/*.csv

# Start TCP Streaming Server
# Simulates real-time data feed (keep running)
python ingestion/tcp_log_file_streaming_server.py

# Expected output:
# TCP server listening on localhost:9999...
# Waiting for new client...

# In new terminal (requires Java 11 environment)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3 \
  --driver-memory 4g \
  ingestion/spark_processing.py

# Successful startup shows:
# Streaming query made progress: ...

```

### 🔍 Key Features Implemented

## Data Ingestion

✅ Historical S3 data → Local CSV files

✅ TCP server simulates real-time streaming

✅ Spark Structured Streaming with 1-hour watermark

## Data Processing

🧹 Automatic schema validation

🕒 Timestamp normalization (UTC conversion)

🔀 Pivot PM2.5/PM10/NO2 metrics → Columns

🌦 Weather data join (static CSV)

## Quality Assurance

📊 Per-batch statistics (count, avg, min/max)

🚫 Invalid record filtering

📂 Output validation (Parquet + checkpoints)

## Section 1 Completion Checklist

✅All CSV files downloaded to data/pending

✅TCP server streaming data to port 9999

✅Spark job writing Parquet files

✅Quality metrics visible in Spark console

✅Processed files moved to data/processed

# 📄 README — Section 2: Data Aggregations, Transformations & Trend Analysis

---

## 📚 Overview

In this section, the dataset generated from **Section 1** is further enhanced through critical **data cleaning**, **normalization**, **feature engineering**, and **trend-based transformations**.  
The resulting dataset is made ready for **analytical exploration** and **machine learning modeling**.

---

## 🛠 Workflow Summary

| Step | Task                                        | Description                                                                                                  |
| :--- | :------------------------------------------ | :----------------------------------------------------------------------------------------------------------- |
| 1    | Outlier Handling & Missing Value Imputation | Cap extreme PM2.5 values, impute missing PM2.5 with mean, and randomly generate missing temperature/humidity |
| 2    | Normalization                               | Standardize PM2.5, temperature, and humidity using Z-Score normalization                                     |
| 3    | Daily Aggregation                           | Group data by day and location, computing daily averages                                                     |
| 4    | Hourly Aggregation                          | Group data by hour and location, computing hourly averages                                                   |
| 5    | Feature Engineering                         | Add rolling averages, lag features, and rate-of-change calculations                                          |

Each step’s output is saved in **both Parquet and CSV formats**.

---

## 📂 Output Structure

    output/
    ├── section2_step1_cleaned/               # After handling outliers and missing values
    ├── section2_step1_cleaned_csv/
    ├── section2_step2_normalized/             # After feature normalization
    ├── section2_step2_normalized_csv/
    ├── section2_step3_daily_agg/              # Daily aggregated data
    ├── section2_step3_daily_agg_csv/
    ├── section2_step4_hourly_agg/             # Hourly aggregated data
    ├── section2_step4_hourly_agg_csv/
    ├── section2_step5_feature_enhanced/       # Final feature-enhanced dataset
    ├── section2_step5_feature_enhanced_csv/

---

## 📈 Detailed Task Breakdown

### 1. Handle Outliers and Missing Values

- PM2.5 readings below 0 are treated as missing and imputed.
- PM2.5 values above 500 are capped at 500.
- Random values are generated for missing:
  - **Temperature** (20°C–35°C)
  - **Humidity** (30%–80%)
- Missing PM2.5 values are filled with the column mean.

✅ **Output:**  
`output/section2_step1_cleaned/` (Parquet)  
`output/section2_step1_cleaned_csv/` (CSV)

---

### 2. Normalize (Z-Score Standardization)

- Apply Z-Score normalization to:
  - PM2.5
  - Temperature
  - Humidity
- Formula used: z = (value - mean) / standard deviation

✅ **Output:**  
`output/section2_step2_normalized/` (Parquet)  
`output/section2_step2_normalized_csv/` (CSV)

---

### 3. Daily Aggregations

- Group records by `location_id` and `date`.
- Calculate daily averages:
- Average PM2.5
- Average Temperature
- Average Humidity

✅ **Output:**  
`output/section2_step3_daily_agg/` (Parquet)  
`output/section2_step3_daily_agg_csv/` (CSV)

---

### 4. Hourly Aggregations

- Group records by `location_id` and `hour`.
- Calculate hourly averages:
- Average PM2.5
- Average Temperature
- Average Humidity

✅ **Output:**  
`output/section2_step4_hourly_agg/` (Parquet)  
`output/section2_step4_hourly_agg_csv/` (CSV)

---

### 5. Feature Engineering: Rolling, Lagging, Rate of Change

- Compute **Rolling Average** (3-row window) for PM2.5.
- Create **Lag Features**:
- Previous PM2.5, Temperature, Humidity
- Calculate **Rate-of-Change**:
- PM2.5, Temperature, and Humidity difference compared to previous timestamp.

✅ **Output:**  
`output/section2_step5_feature_enhanced/` (Parquet)  
`output/section2_step5_feature_enhanced_csv/` (CSV)

---

## 🧹 Key Improvements to the Dataset

| Feature Added                   | Why It Matters                                                             |
| :------------------------------ | :------------------------------------------------------------------------- |
| Outlier Treatment               | Eliminates distortion from extreme sensor readings                         |
| Missing Value Imputation        | Ensures completeness for reliable analysis                                 |
| Z-Score Normalization           | Standardizes features for better statistical analysis and machine learning |
| Daily/Hourly Aggregation        | Identifies broader temporal trends                                         |
| Rolling, Lag, and Rate Features | Captures short-term changes and trend momentum                             |

---

## 🏁 Final Outcome

A **cleaned**, **normalized**, and **feature-rich** dataset with:

- No distortions from missing/outlier values
- Properly scaled numerical fields
- Aggregated daily and hourly trends
- Short-term trend capturing using rolling averages and lag features

This feature-enhanced dataset is now fully ready for:

- Spark SQL queries
- Predictive machine learning models
- Time-series analysis

---

# 📢 Notes

- Temperature and humidity are **randomly generated** for missing values to simulate realistic environmental conditions.
- Rolling averages use a **3-record window** to balance noise and trend capture.
- The project is designed for **expandability** (additional features like moving medians, trend shifts can be easily added).

---

# 📈 Section 2 Processing Pipeline (Visual)

[Input from Section 1 (Parquet)]  
 ⬇  
 [Handle Outliers and Fill Missing Values]  
 ⬇  
 [Z-Score Normalization]  
 ⬇  
 [Daily Aggregation]  
 ⬇  
 [Hourly Aggregation]  
 ⬇  
 [Add Rolling Averages, Lags, Rates]  
 ⬇  
 [Final Feature-Enhanced Dataset]

# ✅ Quick Run Instruction

Once Section 1 files are ready, run:  
 spark-submit Section2/data_agg_transf_trend_analysis.py

# Section 3: Spark SQL Exploration & AQI Classification

📚 **Overview**  
In this section, we perform advanced SQL-based exploration, temporal trend analysis, and Air Quality Index (AQI) classification using the feature-enhanced dataset from Section 2.

---

## 🛠 Workflow Summary

| Step | Task                                     | Description                                                                      |
| :--- | :--------------------------------------- | :------------------------------------------------------------------------------- |
| 1    | Register DataFrame as SQL Temporary View | Enable SQL querying on feature-enhanced data.                                    |
| 2    | Top 5 Polluted Regions                   | Identify regions with the highest average PM2.5 concentrations.                  |
| 3    | Peak Pollution Hours                     | Find the hours of the day when PM2.5 levels are highest.                         |
| 4    | Trend Analysis                           | Use SQL Window Functions to track PM2.5 changes over time.                       |
| 5    | AQI Classification                       | Categorize air quality into Good, Moderate, or Unhealthy using PM2.5 thresholds. |
| 6    | Save Outputs                             | Persist the AQI-classified data to CSV and Parquet formats.                      |

---

## 📂 Output Structure

output/ └── Section3_Output/ ├── section3_aqi_classified_csv/ # AQI-classified data in CSV └── section3_aqi_classified_parquet/ # AQI-classified data in Parquet

Each output contains the AQI Category column added to the original data.

---

## 🔥 Key Features Implemented

| Feature            | Details                                                             |
| :----------------- | :------------------------------------------------------------------ |
| SQL-Based Analysis | Complex Spark SQL queries for temporal and spatial pollution trends |
| Window Functions   | Track PM2.5 rate of change over time using LAG()                    |
| Custom UDF         | Classify air quality levels based on PM2.5 concentrations           |
| Structured Output  | Save results to CSV and Parquet formats for flexible downstream use |

---

## 🧪 Important Queries & Functions

- **Top Regions:**

```sql
SELECT location_id, AVG(pm25) AS avg_pm25
FROM air_quality_data
GROUP BY location_id
ORDER BY avg_pm25 DESC
LIMIT 5
Peak Pollution Hours:


SELECT hour(event_time) AS pollution_hour, AVG(pm25) AS avg_pm25
FROM air_quality_data
GROUP BY pollution_hour
ORDER BY avg_pm25 DESC
LIMIT 5
Trend Detection (Window Function):


Window.partitionBy("location_id").orderBy("event_time")
AQI Classification UDF Logic:

if pm25 <= 50:
    return "Good"
elif pm25 <= 100:
    return "Moderate"
else:
    return "Unhealthy"
🏁 Final Outcome
By the end of Section 3:

You have deep insights into where, when, and how air quality worsens.

A new AQI-classified dataset is created.

Structured outputs ready for visualization, dashboards, or ML models.

✅ Quick Run Instruction
After generating Section 2 outputs, run:


spark-submit Section3/Section3_sql_exploration_and_aqi_classification.py
Outputs will be saved automatically in output/Section3_Output/.

📢 Notes
AQI classification is simplified based on PM2.5 for ease of interpretation.

Outputs are saved both in human-readable CSV and optimized Parquet formats.

This step prepares the dataset for downstream tasks like dashboard visualization or alerts.
```
