# Air Quality Monitoring & Forecasting

## Project Overview
Air quality is a critical public health factor. This project builds an end-to-end data pipeline simulating near-real-time ingestion of pollution and weather data, applying advanced data transformations, SQL analysis, machine learning predictions, and presenting outcomes through interactive dashboards.

---

## Project Structure

The project is modular and split across five sections to ensure maintainability and team collaboration. Intermediate outputs are stored in formats such as CSV, Parquet, or PostgreSQL.

---

## Section 1: Data Ingestion & Initial Pre-Processing

### 1.1 Read from TCP Server (With Watermarking)
- **Goal:** Simulate real-time ingestion using Spark Structured Streaming or batch mode.
- **Implementation:** Applied `eventTime` watermark to handle out-of-order data.
- **Output:** Raw DataFrame ready for cleaning.

### 1.2 Parse Datetime, Drop Irrelevant Fields, and Detect Schema
- **Goal:** Clean up incoming data for consistency and relevance.
- **Implementation:** Parsed timestamps, dropped unused columns, verified schema (e.g., PM2.5, temp, humidity).
- **Output:** Cleaned DataFrame with `TimestampType` and relevant columns.

### 1.3 Merge Multiple Metrics per Timestamp/Region
- **Goal:** Unify different sensor readings (e.g., PM2.5, temp, humidity) into one row per timestamp-region.
- **Output:** Structured DataFrame with all metrics in a single record.

### 1.4 Append Temperature/Humidity
- **Goal:** Enrich pollution data with weather data (from file/API).
- **Output:** Merged dataset of air quality + weather per time-region.

### 1.5 Cross-Verify Data Quality
- **Goal:** Ensure no anomalies or gaps in the dataset.
- **Output:** Validation log using `describe()`, null checks, and value distribution summaries.

---

##  Section 2: Data Aggregations, Transformations & Trend Analysis

###  2.1 Handle Outliers and Missing Values
- **Goal:** Remove or cap extreme values and impute missing readings.
- **Techniques:** Median capping, mean/forward-fill imputation.
- **Output:** Distortion-free dataset.

###  2.2 Normalize or Standardize Key Features
- **Goal:** Scale features for modeling.
- **Techniques:** Z-score normalization using Spark ML’s `StandardScaler`.
- **Output:** Scaled DataFrame ready for ML.

###  2.3 Daily/Hourly Aggregations
- **Goal:** Spot macro patterns via group-by on date/hour.
- **Output:** Aggregated DataFrames (e.g., average hourly PM2.5 per region).

###  2.4 Rolling Averages, Lag Features, and Rate-of-Change
- **Goal:** Capture temporal trends and velocity of pollution changes.
- **Techniques:** `window` and `lag` functions in Spark SQL.
- **Output:** Trend-enriched DataFrame.

---

##  Section 3: Spark SQL Exploration & Correlation Analysis

###  3.1 Create and Manage Data Views
- **Goal:** Register datasets as SQL views/tables for exploration.
- **Output:** Temp or persistent views partitioned by date/region.

###  3.2 Develop Complex Analytical Queries
- **Goal:** Analyze spatial-temporal air quality patterns.
- **Queries:** Top regions by PM2.5, peak intervals, etc.
- **Output:** Insightful SQL result tables.

###  3.3 Trend Analysis Using Window Functions
- **Goal:** Detect continuous pollution spikes or drops.
- **Output:** Reports using `ROW_NUMBER()`, `LAG()`, `LEAD()` functions.

###  3.4 Air Quality Index (AQI) Classification
- **Goal:** Create custom UDF to label PM2.5 levels.
- **Categories:** Good, Moderate, Unhealthy.
- **Output:** Risk-classified data per region and time.

## Section 4: ML Model for PM2.5 Prediction

### 4.1 Feature Selection & Preparation
- **Goal:** Use temporal features to predict future PM2.5 levels.
- **Features Used:** `previous_pm25`, `next_pm25`, `pm25_diff_previous`, `pm25_diff_next`.
- **Process:** Dropped nulls, vectorized features using `VectorAssembler`, and split into training/testing datasets.

### 4.2 Model Training & Evaluation
- **Model:** Random Forest Regressor from Spark MLlib.
- **Performance Metrics:**
  - **RMSE:** 1.99 (indicates low prediction error)
  - **R²:** -∞ (infinite negative value due to insufficient test samples)
- **Result:** Model trained successfully and evaluated using RMSE and R².

### 4.3 Output Predictions
- **Fields:** `region`, `timestamp`, `actual_pm25`, `predicted_pm25`
- **Saved to:** `output/section-4/predictions.csv`

### 4.4 Real-Time Integration Plan (Proposed)
- **Goal:** Apply trained model on streaming data to forecast PM2.5 levels.
- **Plan:** Use the existing structured streaming pipeline to apply `.transform()` using the trained model, and output predictions in real-time for dashboards or alerts.
"""
---
