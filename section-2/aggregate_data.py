from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, stddev, mean, lag, hour, to_date, window
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("AirQualityAggregation").getOrCreate()

# Load CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("output/step2_normalized")

# Convert timestamp column
df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

# Drop rows with null timestamps or region
df = df.dropna(subset=["timestamp", "region"])

# --- Handle Outliers ---
def cap_outliers(df, col_name):
    quantiles = df.approxQuantile(col_name, [0.01, 0.99], 0.0)
    return df.withColumn(col_name, F.when(col(col_name) < quantiles[0], quantiles[0])
                                   .when(col(col_name) > quantiles[1], quantiles[1])
                                   .otherwise(col(col_name)))

for col_name in ["pm25", "temp", "humidity"]:
    df = cap_outliers(df, col_name)

# --- Handle Missing Values ---
df = df.fillna({
    "pm25": df.select(mean("pm25")).first()[0],
    "temp": df.select(mean("temp")).first()[0],
    "humidity": df.select(mean("humidity")).first()[0]
})

# --- Standardize (Z-score normalization) ---
for col_name in ["pm25", "temp", "humidity"]:
    mean_val = df.select(mean(col_name)).first()[0]
    std_val = df.select(stddev(col_name)).first()[0]
    df = df.withColumn(col_name, (col(col_name) - mean_val) / std_val)

# --- Time-based columns ---
df = df.withColumn("date", to_date("timestamp")).withColumn("hour", hour("timestamp"))

# --- Daily Aggregation ---
df_daily = df.groupBy("date", "region").agg(
    avg("pm25").alias("avg_pm25"),
    avg("temp").alias("avg_temp"),
    avg("humidity").alias("avg_humidity")
)

df_daily.write.mode("overwrite").csv("output/step3_daily_aggregate", header=True)

# --- Hourly Aggregation ---
df_hourly = df.groupBy(window("timestamp", "1 hour"), "region").agg(
    avg("pm25").alias("avg_pm25"),
    avg("temp").alias("avg_temp"),
    avg("humidity").alias("avg_humidity")
)

# Flatten window struct
df_hourly = df_hourly.withColumn("window_start", col("window").start).withColumn("window_end", col("window").end).drop("window")
df_hourly.write.mode("overwrite").csv("output/step3_hourly_aggregate", header=True)

# --- Rolling Averages, Lag, Rate of Change ---
window_spec = Window.partitionBy("region").orderBy("timestamp").rowsBetween(-2, 0)
df = df.withColumn("pm25_rolling_avg", F.avg("pm25").over(window_spec))

lag_window = Window.partitionBy("region").orderBy("timestamp")
df = df.withColumn("pm25_lag1", lag("pm25", 1).over(lag_window))
df = df.withColumn("pm25_diff", col("pm25") - col("pm25_lag1"))

# Final enhanced dataset
df.write.mode("overwrite").csv("output/step3_enhanced_features")

spark.stop()
