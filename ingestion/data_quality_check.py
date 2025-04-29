from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Check") \
    .getOrCreate()

# Load the cleaned CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("output/output1.csv")

# Rename problematic column
df_clean = df.withColumnRenamed("PM2.5", "pm25")

# Print the schema
print("Schema:")
df_clean.printSchema()

# Show summary statistics for key metrics
print("\n Summary Statistics:")
df_clean.select("pm25", "temp", "humidity").describe().show()

# Store the cleaned DataFrame in CSV format
df_clean.write.option("header", True).csv("output/cleaned_metrics.csv")
