from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when

spark = SparkSession.builder.appName("Handle Outliers and Missing").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("output/output1.csv")
df = df.withColumnRenamed("PM2.5", "pm25")

# Cap outliers
df = df.withColumn("pm25", when(col("pm25") > 500, 500).otherwise(col("pm25")))

# Impute missing values
for feature in ["pm25", "temp", "humidity"]:
    mean_val = df.select(mean(feature)).first()[0]
    df = df.fillna({feature: mean_val})

df.write.mode("overwrite").csv("output/step1_cleaned", header=True)