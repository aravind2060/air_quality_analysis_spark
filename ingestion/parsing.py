from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Step 1: Create Spark session
spark = SparkSession.builder.appName("ParseAndCleanAirQuality").getOrCreate()

# Step 2: Load original CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("pollution_data.csv")

# Step 3: Print original schema
print("Original Schema:")
df.printSchema()

# Step 4: Convert 'timestamp' column to Spark TimestampType
df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Step 5: Filter only required metrics
valid_metrics = ["PM2.5", "temp", "humidity"]
df_filtered = df.filter(df["metric"].isin(valid_metrics))

# Step 6: Show cleaned schema and few records
print("Filtered & Parsed Schema:")
df_filtered.printSchema()
df_filtered.show(truncate=False)

# Step 7: Save cleaned data to new CSV
df_filtered.write.mode("overwrite").option("header", True).csv("output/parsing.csv")
