from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AirQualityIngestion") \
    .getOrCreate()

# Define schema for incoming data
open_aq_schema = StructType([
    StructField("location_id", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("utc_timestamp", TimestampType()),
    StructField("parameter", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# Read from TCP socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse the raw CSV lines
def parse_csv(line):
    parts = line.split(",")
    if len(parts) == 9:
        return (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8])
    else:
        return None

parse_udf = udf(parse_csv, open_aq_schema)

parsed_stream = raw_stream.withColumn("parsed", parse_udf(col("value"))).select("parsed.*")

# Clean the data
parsed_stream = parsed_stream \
    .withColumn("utc_timestamp", to_timestamp(col("utc_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("value", col("value").cast(DoubleType())) \
    .withColumn("latitude", col("latitude").cast(DoubleType())) \
    .withColumn("longitude", col("longitude").cast(DoubleType()))

# Optional filter if needed
air_quality = parsed_stream.filter(
    col("parameter").isin(["pm25", "pm10", "no2"])
).withWatermark("utc_timestamp", "1 hour")

# Read static weather data
weather_schema = StructType([
    StructField("location_id", StringType()),
    StructField("utc_timestamp", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

weather_df = spark.read \
    .schema(weather_schema) \
    .csv("./weather_data.csv") \
    .withWatermark("utc_timestamp", "1 hour")

# Join air quality and weather
merged_stream = air_quality.join(
    weather_df,
    on=["location_id", "utc_timestamp"],
    how="leftOuter"
)

# Data quality check
def check_data_quality(df, epoch_id):
    if df.count() > 0:
        print(f"\nData Quality Check for Batch {epoch_id}:")
        df.select(
            count("*").alias("total_records"),
            min("utc_timestamp").alias("oldest_timestamp"),
            max("utc_timestamp").alias("newest_timestamp"),
            avg("value").alias("avg_parameter_value"),
            avg("temperature").alias("avg_temp")
        ).show()

# Write stream to storage
query = merged_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(check_data_quality) \
    .format("parquet") \
    .option("path", "./output/processed_data") \
    .option("checkpointLocation", "./checkpoints/air_quality") \
    .trigger(once=True) \
    .start()

query.awaitTermination()