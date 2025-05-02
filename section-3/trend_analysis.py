from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lag, lead, col
from pyspark.sql.window import Window

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Air Quality Trend Analysis with Window Functions") \
    .getOrCreate()

# Step 2: Load the processed dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("output/cleaned_metrics.csv")

# Step 3: Rename PM2.5 column if necessary
df = df.withColumnRenamed("PM2.5", "pm25")

# Step 4: Convert timestamp column
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Optional: Inspect schema and preview
print("Column names:", df.columns)
df.select("timestamp", "region", "pm25").show(10, truncate=False)

# Step 5: Define window for trend comparison
windowSpec = Window.partitionBy("region").orderBy("timestamp")

# Step 6: Apply lag and lead for comparison
df_with_trends = df.withColumn("previous_pm25", lag("pm25", 1).over(windowSpec)) \
    .withColumn("next_pm25", lead("pm25", 1).over(windowSpec)) \
    .withColumn("pm25_diff_previous", col("pm25") - col("previous_pm25")) \
    .withColumn("pm25_diff_next", col("next_pm25") - col("pm25"))

# # Optional: Save the full trend data for debugging
# df_with_trends.write.option("header", "true").csv("output/section-3/debug_trend_data.csv")

# Step 7: Register table and run trend query with relaxed threshold
df_with_trends.createOrReplaceTempView("air_quality_trends")

query = """
SELECT 
    region,
    timestamp,
    pm25,
    previous_pm25,
    next_pm25,
    pm25_diff_previous,
    pm25_diff_next
FROM air_quality_trends
WHERE abs(pm25_diff_previous) > 0.5 OR abs(pm25_diff_next) > 0.5
ORDER BY region, timestamp
"""

print("🔥 Trend analysis indicating sustained pollution increases or anomalies:")
trend_result = spark.sql(query)

# Show and save result
trend_result.show(truncate=False)
trend_result.write.option("header", "true").csv("output/section-3/trend_analysis_with_anomalies.csv")
