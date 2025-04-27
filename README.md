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
