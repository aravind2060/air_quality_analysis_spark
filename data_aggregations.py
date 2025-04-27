# SECTION 2: Data Aggregation, Transformation & Trend Analysis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, avg, lag, date_trunc
from pyspark.sql.window import Window

# ----------------------------------------------------------------------------
# 1. Start Spark Session
# ----------------------------------------------------------------------------

spark = SparkSession.builder \
    .appName("AirQuality_Section2_Transformations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------------
# 2. Load Cleaned Data (from Section 1 Output - Parquet)
# ----------------------------------------------------------------------------

input_path = "/Section1/output/section1_cleaned/"
df = spark.read.parquet(input_path)

print("✅ Loaded data from Section 1:", df.columns)

# ----------------------------------------------------------------------------
# 3. Handle Outliers and Missing Values
# ----------------------------------------------------------------------------

# Filtering extreme outliers (logical thresholds)
df_cleaned = df.filter((col("pm25").isNull()) | ((col("pm25") >= 0) & (col("pm25") <= 500))) \
               .filter((col("humidity").isNull()) | ((col("humidity") >= 0) & (col("humidity") <= 100))) \
               .filter((col("temperature").isNull()) | ((col("temperature") >= -50) & (col("temperature") <= 60)))

# Impute missing values using mean
means = df_cleaned.select(
    mean("pm25").alias("mean_pm25"),
    mean("temperature").alias("mean_temperature"),
    mean("humidity").alias("mean_humidity")
).collect()[0]

df_imputed = df_cleaned.fillna({
    "pm25": means['mean_pm25'],
    "temperature": means['mean_temperature'],
    "humidity": means['mean_humidity']
})

print("✅ Outliers handled and missing values imputed.")

# ----------------------------------------------------------------------------
# 4. Normalize or Standardize Key Features
# ----------------------------------------------------------------------------

# Compute mean and stddev for normalization
stats = df_imputed.select(
    mean("pm25").alias("mean_pm25"), stddev("pm25").alias("std_pm25"),
    mean("temperature").alias("mean_temperature"), stddev("temperature").alias("std_temperature"),
    mean("humidity").alias("mean_humidity"), stddev("humidity").alias("std_humidity")
).collect()[0]

# Apply Z-score normalization
df_normalized = df_imputed.withColumn("pm25_normalized", (col("pm25") - stats["mean_pm25"]) / stats["std_pm25"]) \
                          .withColumn("temperature_normalized", (col("temperature") - stats["mean_temperature"]) / stats["std_temperature"]) \
                          .withColumn("humidity_normalized", (col("humidity") - stats["mean_humidity"]) / stats["std_humidity"])

print("✅ Features normalized.")

# ----------------------------------------------------------------------------
# 5. Daily and Hourly Aggregations
# ----------------------------------------------------------------------------

# Hourly aggregation
hourly_agg = df_normalized.groupBy(
    date_trunc("hour", "event_time").alias("hour"),
    "location_id"
).agg(
    avg("pm25").alias("avg_pm25_hour"),
    avg("temperature").alias("avg_temperature_hour"),
    avg("humidity").alias("avg_humidity_hour")
)

# Daily aggregation
daily_agg = df_normalized.groupBy(
    date_trunc("day", "event_time").alias("day"),
    "location_id"
).agg(
    avg("pm25").alias("avg_pm25_day"),
    avg("temperature").alias("avg_temperature_day"),
    avg("humidity").alias("avg_humidity_day")
)

print("✅ Daily and Hourly aggregations computed.")

# ----------------------------------------------------------------------------
# 6. Rolling Averages, Lag Features, Rate of Change
# ----------------------------------------------------------------------------

# Define window for rolling average and lag features
windowSpec = Window.partitionBy("location_id").orderBy("event_time").rowsBetween(-2, 0)

df_features = df_normalized.withColumn("rolling_pm25", avg("pm25").over(windowSpec)) \
                           .withColumn("lag_pm25", lag("pm25", 1).over(windowSpec)) \
                           .withColumn("rate_of_change_pm25", (col("pm25") - lag("pm25", 1).over(windowSpec)))

print("✅ Rolling averages, lag features, and rate of change computed.")

# ----------------------------------------------------------------------------
# 7. Save Outputs
# ----------------------------------------------------------------------------

# Set output path
output_base = "output/"

# Save full feature-enhanced dataset
df_features.write.mode("overwrite").parquet(output_base + "full_features/")

# Save hourly aggregations
hourly_agg.write.mode("overwrite").parquet(output_base + "hourly_aggregations/")

# Save daily aggregations
daily_agg.write.mode("overwrite").parquet(output_base + "daily_aggregations/")

print("✅ All outputs saved successfully at Section 2 Output folder.")

# ----------------------------------------------------------------------------
# 📋 Completion Summary
# ----------------------------------------------------------------------------

print("\n✅ Section 2 Completed Successfully!")
print("""
Outputs:
- Full feature-enhanced dataset → Section 2/output/full_features/
- Hourly aggregation → Section 2/output/hourly_aggregations/
- Daily aggregation → Section 2/output/daily_aggregations/
""")