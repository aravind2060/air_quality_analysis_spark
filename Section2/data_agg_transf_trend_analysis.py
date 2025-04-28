# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when, mean as mean_, stddev as stddev_, avg, hour, to_date, lag, round as round_, monotonically_increasing_id
# from pyspark.sql.window import Window
# import random

# # 1. Start Spark Session
# spark = SparkSession.builder \
#     .appName("AirQualitySection2") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # 2. Load Section 1 output
# df = spark.read.parquet("ingestion/output/final_single_parquet/")  # Path to your parquet

# # 3. Handle Outliers and Missing Values
# # Cap pm25 to 0–500 (reasonable range)
# df = df.withColumn("pm25", when(col("pm25") < 0, None).otherwise(col("pm25")))
# df = df.withColumn("pm25", when(col("pm25") > 500, 500.0).otherwise(col("pm25")))

# # Randomly generate temperature and humidity where missing
# def generate_random_temp():
#     return float(random.randint(20, 35))  # reasonable range

# def generate_random_humidity():
#     return float(random.randint(30, 80))  # reasonable range

# from pyspark.sql.functions import udf
# from pyspark.sql.types import DoubleType

# generate_temp_udf = udf(generate_random_temp, DoubleType())
# generate_humidity_udf = udf(generate_random_humidity, DoubleType())

# df = df.withColumn(
#     "temperature",
#     when(col("temperature").isNull(), generate_temp_udf()).otherwise(col("temperature"))
# ).withColumn(
#     "humidity",
#     when(col("humidity").isNull(), generate_humidity_udf()).otherwise(col("humidity"))
# )

# # Impute missing PM2.5 with column mean (mean imputation)
# pm25_mean = df.select(mean_("pm25")).collect()[0][0]
# df = df.fillna({"pm25": pm25_mean})

# #  Now df is cleaned + enriched

# # 4. Normalize or Standardize (Z-score normalization)
# stats = df.select(
#     mean_("pm25").alias("pm25_mean"), stddev_("pm25").alias("pm25_std"),
#     mean_("temperature").alias("temperature_mean"), stddev_("temperature").alias("temperature_std"),
#     mean_("humidity").alias("humidity_mean"), stddev_("humidity").alias("humidity_std")
# ).collect()[0]

# df = df.withColumn(
#     "pm25_zscore", ((col("pm25") - stats["pm25_mean"]) / stats["pm25_std"])
# ).withColumn(
#     "temperature_zscore", ((col("temperature") - stats["temperature_mean"]) / stats["temperature_std"])
# ).withColumn(
#     "humidity_zscore", ((col("humidity") - stats["humidity_mean"]) / stats["humidity_std"])
# )

# #  Now df has scaled columns

# # 5. Daily and Hourly Aggregations

# #  Daily Aggregation
# daily_df = df.withColumn("date", to_date(col("event_time"))) \
#     .groupBy("date", "location_id") \
#     .agg(
#         avg("pm25").alias("avg_pm25"),
#         avg("temperature").alias("avg_temperature"),
#         avg("humidity").alias("avg_humidity")
#     )

# #  Hourly Aggregation
# hourly_df = df.withColumn("hour", hour(col("event_time"))) \
#     .groupBy("hour", "location_id") \
#     .agg(
#         avg("pm25").alias("avg_pm25"),
#         avg("temperature").alias("avg_temperature"),
#         avg("humidity").alias("avg_humidity")
#     )

# #  Now we have aggregated views daily and hourly

# # 6. Rolling Averages, Lag Features, Rate of Change

# # Create a Window Spec for each location_id, ordered by event_time
# window_spec = Window.partitionBy("location_id").orderBy("event_time")

# #  Rolling average (previous 3 records)
# df = df.withColumn(
#     "rolling_pm25_avg", round_(avg("pm25").over(window_spec.rowsBetween(-2, 0)), 2)
# )

# #  Lag features
# df = df.withColumn(
#     "pm25_lag1", lag("pm25", 1).over(window_spec)
# ).withColumn(
#     "temperature_lag1", lag("temperature", 1).over(window_spec)
# ).withColumn(
#     "humidity_lag1", lag("humidity", 1).over(window_spec)
# )

# #  Rate of change
# df = df.withColumn(
#     "pm25_rate_change", (col("pm25") - col("pm25_lag1"))
# ).withColumn(
#     "temperature_rate_change", (col("temperature") - col("temperature_lag1"))
# ).withColumn(
#     "humidity_rate_change", (col("humidity") - col("humidity_lag1"))
# )

# # Now df is feature-enhanced with rolling averages, lags, and rate of change!

# # 7. Save the final feature-enhanced dataset
# df.write.mode("overwrite").parquet("output/section2_feature_enhanced/")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean as mean_, stddev as stddev_, avg, hour, to_date, lag, round as round_, monotonically_increasing_id
from pyspark.sql.window import Window
import random
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Start Spark Session
spark = SparkSession.builder \
    .appName("AirQualitySection2_Full") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Load Section 1 output
df = spark.read.parquet("ingestion/output/final_single_parquet/")

# 2. Handle Outliers and Missing Values
df = df.withColumn("pm25", when(col("pm25") < 0, None).otherwise(col("pm25")))
df = df.withColumn("pm25", when(col("pm25") > 500, 500.0).otherwise(col("pm25")))

# Randomly generate temp/humidity where missing
def generate_random_temp():
    return float(random.randint(20, 35))

def generate_random_humidity():
    return float(random.randint(30, 80))

generate_temp_udf = udf(generate_random_temp, DoubleType())
generate_humidity_udf = udf(generate_random_humidity, DoubleType())

df = df.withColumn(
    "temperature",
    when(col("temperature").isNull(), generate_temp_udf()).otherwise(col("temperature"))
).withColumn(
    "humidity",
    when(col("humidity").isNull(), generate_humidity_udf()).otherwise(col("humidity"))
)

# Impute missing PM2.5 with mean
pm25_mean = df.select(mean_("pm25")).collect()[0][0]
df = df.fillna({"pm25": pm25_mean})

# Save Step 1 outputs
df.write.mode("overwrite").parquet("output/section2_step1_cleaned/")
df.write.mode("overwrite").option("header", "true").csv("output/section2_step1_cleaned_csv/")

# 3. Normalize or Standardize (Z-score normalization)
stats = df.select(
    mean_("pm25").alias("pm25_mean"), stddev_("pm25").alias("pm25_std"),
    mean_("temperature").alias("temperature_mean"), stddev_("temperature").alias("temperature_std"),
    mean_("humidity").alias("humidity_mean"), stddev_("humidity").alias("humidity_std")
).collect()[0]

df_normalized = df.withColumn(
    "pm25_zscore", ((col("pm25") - stats["pm25_mean"]) / stats["pm25_std"])
).withColumn(
    "temperature_zscore", ((col("temperature") - stats["temperature_mean"]) / stats["temperature_std"])
).withColumn(
    "humidity_zscore", ((col("humidity") - stats["humidity_mean"]) / stats["humidity_std"])
)

# Save Step 2 outputs
df_normalized.write.mode("overwrite").parquet("output/section2_step2_normalized/")
df_normalized.write.mode("overwrite").option("header", "true").csv("output/section2_step2_normalized_csv/")

# 4. Daily Aggregations
daily_df = df_normalized.withColumn("date", to_date(col("event_time"))) \
    .groupBy("date", "location_id") \
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Save Step 3 Daily Aggregation
daily_df.write.mode("overwrite").parquet("output/section2_step3_daily_agg/")
daily_df.write.mode("overwrite").option("header", "true").csv("output/section2_step3_daily_agg_csv/")

# 5. Hourly Aggregations
hourly_df = df_normalized.withColumn("hour", hour(col("event_time"))) \
    .groupBy("hour", "location_id") \
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Save Step 4 Hourly Aggregation
hourly_df.write.mode("overwrite").parquet("output/section2_step4_hourly_agg/")
hourly_df.write.mode("overwrite").option("header", "true").csv("output/section2_step4_hourly_agg_csv/")

# 6. Rolling Averages, Lag Features, and Rate of Change
window_spec = Window.partitionBy("location_id").orderBy("event_time")

df_features = df_normalized.withColumn(
    "rolling_pm25_avg", round_(avg("pm25").over(window_spec.rowsBetween(-2, 0)), 2)
).withColumn(
    "pm25_lag1", lag("pm25", 1).over(window_spec)
).withColumn(
    "temperature_lag1", lag("temperature", 1).over(window_spec)
).withColumn(
    "humidity_lag1", lag("humidity", 1).over(window_spec)
).withColumn(
    "pm25_rate_change", (col("pm25") - col("pm25_lag1"))
).withColumn(
    "temperature_rate_change", (col("temperature") - col("temperature_lag1"))
).withColumn(
    "humidity_rate_change", (col("humidity") - col("humidity_lag1"))
)

# Save Step 5 Final Feature-Enhanced Dataset
df_features.write.mode("overwrite").parquet("output/section2_step5_feature_enhanced/")
df_features.write.mode("overwrite").option("header", "true").csv("output/section2_step5_feature_enhanced_csv/")