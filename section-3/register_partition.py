from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Register Air Quality View") \
    .enableHiveSupport() \
    .getOrCreate()  # <-- This line must align with the above chain

# Step 2: Load pre-processed data
df = spark.read.option("header", True).option("inferSchema", True).csv("output/step4_lag_rolling")

# Step 3: Create 'date' column for partitioning (if timestamp exists)
df = df.withColumn("date", to_date("timestamp"))

# Step 4: Register as Temporary View for Spark SQL
df.createOrReplaceTempView("air_quality")

# ✅ Optional Step: Save as Partitioned Hive Table (permanent, optimized for SQL)
# df.write.mode("overwrite").partitionBy("date", "region").saveAsTable("air_quality_partitioned")

# Step 5: Verify by querying from the view
print("Sample query from 'air_quality' view:")
spark.sql("SELECT date, region, pm25, temp, humidity FROM air_quality LIMIT 5").show()

# Step 6: Show schema
print("Schema:")
df.printSchema()

# Step 7: Save the final DataFrame to CSV
df.write.option("header", True).mode("overwrite").csv("output/section-3/registered_air_quality_view")
