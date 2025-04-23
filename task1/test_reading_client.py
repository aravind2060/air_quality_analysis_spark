import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, to_timestamp, split

# Build Spark Session
spark: SparkSession = SparkSession \
    .builder \
    .appName("StructuredStreamingAggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# "location_id","sensors_id","location","datetime","lat","lon","parameter","units","value"
# Define Schema

field_names = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
field_types = [IntegerType(), IntegerType(), StringType(), TimestampType(), DoubleType(), DoubleType(), StringType(), StringType(), DoubleType()]

schema = ArrayType(StringType())
# schema = (
#     StructType()
#     .add("location_id", IntegerType())
#     .add("sensors_id", IntegerType())
#     .add("location", StringType())
#     .add("datetime", TimestampType())
#     .add("lat", DoubleType())
#     .add("lon", DoubleType())
#     .add("parameter", StringType())
#     .add("units", StringType())
#     .add("value", DoubleType())
# )

# Read Stream from Socket
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed = lines.select(
    from_json(col("value"), schema).alias("parsed_values")
)

for i, (name, dtype) in enumerate(zip(field_names, field_types)):
    parsed = parsed.withColumn(name, col("parsed_values").getItem(i).cast(dtype))

final = parsed.select(*field_names)

query = (
    parsed.writeStream
    .format("parquet")
    .option("path", "/opt/bitnami/spark/Air/parsed")
    .option("checkpointLocation", f"/opt/bitnami/spark/Air/checkpoint/run_{int(time.time())}")
    .outputMode("append")
    .start()
)

query.awaitTermination()