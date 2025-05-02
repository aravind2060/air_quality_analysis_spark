
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, to_timestamp, hour, dayofmonth
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import plotly.graph_objects as go
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName("AirQualityPipeline").getOrCreate()

# SECTION 1: Ingestion
input_path = "output/section-3/trend_analysis_with_anomalies.csv/part-00000-8b96b38d-9dcb-49ad-90e8-d1b7ec563632-c000.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
df = df.dropna()

# SECTION 2: Transformations
assembler = VectorAssembler(
    inputCols=["previous_pm25", "next_pm25", "pm25_diff_previous", "pm25_diff_next"],
    outputCol="features"
)
df_vector = assembler.transform(df)

# SECTION 3: SQL AQI Classification (basic)
df_vector = df_vector.withColumn(
    "AQI_category",
    when(col("pm25") <= 50, "Good")
    .when((col("pm25") > 50) & (col("pm25") <= 100), "Moderate")
    .otherwise("Unhealthy")
)

# Save AQI Breakdown
aqi_df = df_vector.groupBy("AQI_category").count()
aqi_df.write.mode("overwrite").option("header", True).csv("output/section-5/aqi_breakdown.csv")

# SECTION 4: Model Prediction
final_df = df_vector.select("region", "timestamp", "features", "pm25")
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)
rf = RandomForestRegressor(featuresCol="features", labelCol="pm25", numTrees=100, maxDepth=5)
model = rf.fit(train_df)
predictions = model.transform(test_df)

# Evaluation
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="r2").evaluate(predictions)

# Save model output
results_df = predictions.select("region", "timestamp", col("pm25").alias("actual_pm25"), col("prediction").alias("predicted_pm25"))
results_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/section-5/final_predictions.csv")

# Convert to Pandas for visualization
pandas_df = results_df.toPandas()
pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"])

# SECTION 5: Visualization
fig = go.Figure()
fig.add_trace(go.Scatter(x=pandas_df["timestamp"], y=pandas_df["actual_pm25"], mode='lines+markers', name='Actual PM2.5'))
fig.add_trace(go.Scatter(x=pandas_df["timestamp"], y=pandas_df["predicted_pm25"], mode='lines+markers', name='Predicted PM2.5'))
fig.update_layout(title="Actual vs Predicted PM2.5 Levels", xaxis_title="Timestamp", yaxis_title="PM2.5")
fig.write_html("output/section-5/pm25_predictions_chart.html")

# Save performance report
with open("output/section-5/model_metrics.txt", "w") as f:
    f.write(f"Random Forest Model Performance:\n")
    f.write(f"RMSE: {rmse:.2f}\n")
    f.write(f"R²: {r2:.2f}\n")

spark.stop()
