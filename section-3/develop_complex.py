from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, current_timestamp, col

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Air Quality Trend Analysis") \
    .getOrCreate()

# Step 2: Load the processed dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("output/section-3/registered_air_quality_view")

# Ensure timestamp is properly typed
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Register as a temp view
df.createOrReplaceTempView("air_quality")

# Step 3: Define Current Time (Simulated if testing)
# For real-time use: use current_timestamp()
# For reproducibility (e.g., in batch), you can fix a simulated current time like:
# current_time = spark.sql("SELECT max(timestamp) as now FROM air_quality").collect()[0]['now']

# Step 4: Query - Highest avg PM2.5 in the last 24 hours by region
query1 = """
SELECT 
    region,
    ROUND(AVG(pm25), 2) as avg_pm25_24h
FROM air_quality
WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY region
ORDER BY avg_pm25_24h DESC
LIMIT 1
"""

print("Top 5 regions with highest average PM2.5 over the past 24 hours:")
query1_result = spark.sql(query1)

# Save the result of query1 to CSV
query1_result.write.option("header", "true").csv("output/section-3/highest_avg_pm25_24h.csv")

# Step 5: Query - Peak pollution time intervals
query2 = """
SELECT 
    region,
    HOUR(timestamp) AS hour_of_day,
    ROUND(AVG(pm25), 2) as avg_pm25
FROM air_quality
GROUP BY region, HOUR(timestamp)
ORDER BY avg_pm25 DESC
LIMIT 10
"""

print("Peak pollution intervals by region and hour:")
query2_result = spark.sql(query2)

# Save the result of query2 to CSV
query2_result.write.option("header", "true").csv("output/section-3/peak_pollution_intervals.csv")

