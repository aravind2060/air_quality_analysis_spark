from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev

spark = SparkSession.builder.appName("Normalize Features").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("output/step1_cleaned")

stats = df.select(
    mean("pm25").alias("mean_pm25"),
    stddev("pm25").alias("std_pm25"),
    mean("humidity").alias("mean_humidity"),
    stddev("humidity").alias("std_humidity")
).first()

df = df.withColumn("pm25_z", (col("pm25") - stats["mean_pm25"]) / stats["std_pm25"])
df = df.withColumn("humidity_z", (col("humidity") - stats["mean_humidity"]) / stats["std_humidity"])

df.write.mode("overwrite").csv("output/step2_normalized", header=True)