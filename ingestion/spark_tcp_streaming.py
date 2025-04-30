from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr, trim, first, min as min_, max as max_, count as count_
import time

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("AirQualityTCPStream_SingleFile_Clean") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ➡ Important config fix
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

# 2. Read TCP streaming data
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 3. Clean the incoming data
parsed_df = raw_df.withColumn(
    "value", expr("regexp_replace(regexp_replace(value, '[\\\\[\\\\]]', ''), \"'\", '')")
).withColumn(
    "split_values", expr("split(value, ',\\s*')")
)

# 4. Select correct columns
selected_df = parsed_df.select(
    trim(col("split_values").getItem(0)).alias("location_id"),
    trim(col("split_values").getItem(1)).alias("sensors_id"),
    trim(col("split_values").getItem(2)).alias("location"),
    trim(col("split_values").getItem(3)).alias("datetime"),
    col("split_values").getItem(4).cast("double").alias("latitude"),
    col("split_values").getItem(5).cast("double").alias("longitude"),
    trim(col("split_values").getItem(6)).alias("parameter"),
    trim(col("split_values").getItem(7)).alias("unit"),
    col("split_values").getItem(8).cast("double").alias("value")
)

# 5. Parse datetime and drop unwanted fields
processed_df = selected_df.withColumn(
    "event_time", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
).drop("sensors_id", "location", "unit", "datetime")

# 6. Add watermark
watermarked_df = processed_df.withWatermark("event_time", "5 minutes")

# 7. Pivot the data to merge pm25, temperature, humidity
pivoted_df = watermarked_df.groupBy(
    "location_id", "latitude", "longitude", "event_time"
).pivot("parameter", ["pm25", "temperature", "humidity"]).agg(first("value"))

# 8. Cache the pivoted stream into a temporary view (in memory)
query = pivoted_df.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("pivoted_table") \
    .start()

# 9. Let the stream run for some seconds to collect data
print("Waiting for data collection...")
time.sleep(60)  # adjust time if needed

# 10. Stop the streaming query
query.stop()
print("Data collection stopped. Saving single files...")

# 11. Read collected data from memory
pivoted_final_df = spark.sql("SELECT * FROM pivoted_table")

# 12. Save SINGLE PARQUET and SINGLE CSV

pivoted_final_df.coalesce(1).write.mode("overwrite").parquet("output/final_single_parquet/")
pivoted_final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/final_single_csv/")

print("✅ Single Parquet and CSV saved successfully!")

# 13. Done
spark.stop()