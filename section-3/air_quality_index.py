from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Air Quality Index Classification") \
    .getOrCreate()

# Step 2: Load the processed dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("output/cleaned_metrics.csv")

# Step 3: Define the UDF for AQI classification based on PM2.5 values
def classify_aqi(pm25):
    if pm25 <= 12:
        return "Good"
    elif pm25 <= 35.4:
        return "Moderate"
    else:
        return "Unhealthy"

# Register the UDF
aqi_udf = udf(classify_aqi, StringType())

# Step 4: Apply the UDF to classify AQI for each row based on the pm25 value
df_with_aqi = df.withColumn("AQI", aqi_udf(col("pm25")))

# Step 5: Group by region and rank regions by AQI classification using a CASE expression
df_with_aqi.createOrReplaceTempView("air_quality_with_aqi")

# Step 6: Query to rank regions by AQI classification
query = """
SELECT 
    region,
    AVG(pm25) as avg_pm25,
    COUNT(*) as records_count,
    MAX(AQI) as highest_aqi,
    CASE
        WHEN MAX(AQI) = 'Good' THEN 1
        WHEN MAX(AQI) = 'Moderate' THEN 2
        WHEN MAX(AQI) = 'Unhealthy' THEN 3
    END as aqi_rank
FROM air_quality_with_aqi
GROUP BY region
ORDER BY aqi_rank
"""

# Step 7: Execute the query to get regions ranked by AQI classification
result = spark.sql(query)

# Step 8: Show the result
print("🔥 Ranking regions by their AQI classification:")
result.show(truncate=False)

# Step 9: Optionally save the result to a CSV file
result.write.option("header", "true").csv("output/section-3/region_aqi_ranking.csv")
