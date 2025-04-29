from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Lag and Rolling Features").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("output/step2_normalized")

w = Window.partitionBy("region").orderBy("timestamp")
rolling_w = w.rowsBetween(-2, 0)

df = df.withColumn("pm25_lag1", lag("pm25").over(w))
df = df.withColumn("pm25_change", col("pm25") - col("pm25_lag1"))
df = df.withColumn("pm25_rolling_avg", avg("pm25").over(rolling_w))

df.write.mode("overwrite").csv("output/step4_lag_rolling", header=True)