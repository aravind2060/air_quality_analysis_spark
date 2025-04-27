from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr, trim, first, min as min_, max as max_, count as count_
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("AirQualityTCPStream_CompleteSection1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ➡️ Important config fix
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

# 8. Validation summary (count, min, max)
validation_df = pivoted_df.select(
    count_("location_id").alias("record_count"),
    min_("event_time").alias("earliest_timestamp"),
    max_("event_time").alias("latest_timestamp"),
    min_("pm25").alias("min_pm25"),
    max_("pm25").alias("max_pm25"),
    min_("temperature").alias("min_temperature"),
    max_("temperature").alias("max_temperature"),
    min_("humidity").alias("min_humidity"),
    max_("humidity").alias("max_humidity")
)

# 9. Save pivoted clean data into PARQUET and CSV (normal format) separately

# ➡️ Parquet saving
query_parquet = pivoted_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/section1_cleaned/") \
    .option("checkpointLocation", "output/section1_checkpoint/") \
    .start()

# ➡️ CSV (normal format) saving
query_csv = pivoted_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/section1_cleaned_csv/") \
    .option("checkpointLocation", "output/section1_checkpoint_csv/") \
    .option("header", "true") \
    .start()

# ➡️ Console output for validation
query_validation = validation_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# 10. Wait for all queries
query_parquet.awaitTermination()
query_csv.awaitTermination()
query_validation.awaitTermination()