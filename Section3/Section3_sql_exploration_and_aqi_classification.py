# Section3_sql_exploration_and_aqi_classification.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Air Quality SQL Exploration") \
    .getOrCreate()

# Load the final feature-enhanced dataset
df = spark.read.csv("../output/section2_step5_feature_enhanced_csv/", header=True, inferSchema=True)

# Register as Temp View
df.createOrReplaceTempView("air_quality_data")

# --------------------------------------
# 1. Top 5 most polluted regions
print("Top 5 Most Polluted Regions:")
query_top_regions = """
SELECT location_id, AVG(pm25) AS avg_pm25
FROM air_quality_data
GROUP BY location_id
ORDER BY avg_pm25 DESC
LIMIT 5
"""
top_regions = spark.sql(query_top_regions)
top_regions.show()

# --------------------------------------
# 2. Peak pollution hours
print("Peak Pollution Hours:")
query_peak_hours = """
SELECT hour(event_time) AS pollution_hour, AVG(pm25) AS avg_pm25
FROM air_quality_data
GROUP BY pollution_hour
ORDER BY avg_pm25 DESC
LIMIT 5
"""
peak_hours = spark.sql(query_peak_hours)
peak_hours.show()

# --------------------------------------
# 3. Trend Analysis
print("Trend Analysis (PM2.5 Rate of Change):")
windowSpec = Window.partitionBy("location_id").orderBy("event_time")
df_with_trends = df.withColumn("pm25_change", F.col("pm25") - F.lag("pm25", 1).over(windowSpec))
df_with_trends.select("location_id", "event_time", "pm25", "pm25_change").show(5)

# --------------------------------------
# 4. AQI Classification
print("AQI Classification:")

def classify_aqi(pm25_value):
    if pm25_value <= 50:
        return "Good"
    elif pm25_value <= 100:
        return "Moderate"
    else:
        return "Unhealthy"

# Register UDF
classify_aqi_udf = F.udf(classify_aqi, StringType())

# Apply UDF to DataFrame
df_final = df.withColumn("AQI_Category", classify_aqi_udf(F.col("pm25")))
df_final.select("location_id", "event_time", "pm25", "AQI_Category").show(5)

# --------------------------------------
# 5. Save the output (both CSV and Parquet)
print("Saving the AQI classified DataFrame...")

df_final.write.mode("overwrite").csv("../output/section3_aqi_classified_csv", header=True)
df_final.write.mode("overwrite").parquet("../output/section3_aqi_classified_parquet")

# Optional but recommended: Read it back and verify the save
print("Verifying saved CSV data:")
verified_csv = spark.read.csv("../output/section3_aqi_classified_csv", header=True, inferSchema=True)
verified_csv.show(5)

print("Verifying saved Parquet data:")
verified_parquet = spark.read.parquet("../output/section3_aqi_classified_parquet")
verified_parquet.show(5)

# Stop Spark
spark.stop()
