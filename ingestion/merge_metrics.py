from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Step 1: Start Spark session
spark = SparkSession.builder.appName("MergeMetrics").getOrCreate()

# Step 2: Load the cleaned data
df = spark.read.option("header", True).option("inferSchema", True).csv("output/parsing.csv")

# Step 3: Ensure timestamp is of correct type
df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Step 4: Pivot the metrics
df_pivoted = df.groupBy("timestamp", "region").pivot("metric").agg({"value": "first"})

# Step 5: Show schema and result
print("Merged Schema:")
df_pivoted.printSchema()
df_pivoted.show(truncate=False)

# Step 6: Save result
df_pivoted.write.mode("overwrite").option("header", True).csv("output/output1.csv")
