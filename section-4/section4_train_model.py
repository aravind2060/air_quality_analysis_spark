from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os

# Initialize Spark session
spark = SparkSession.builder.appName("AirQuality_PM25_Regression").getOrCreate()

# Load CSV from processed Section 3 output
file_path = "../output/section-3/trend_analysis_with_anomalies.csv/part-00000*.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

# Drop rows with missing values
df_clean = df.dropna()

# Assemble features
feature_cols = ["previous_pm25", "next_pm25", "pm25_diff_previous", "pm25_diff_next"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_vector = assembler.transform(df_clean)

# Prepare training data
final_df = df_vector.select("features", "pm25", "region", "timestamp")
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)

# Define Random Forest model
rf = RandomForestRegressor(featuresCol="features", labelCol="pm25")

# Set up parameter grid
paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 150]) \
    .addGrid(rf.maxDepth, [3, 5, 7]) \
    .build()

# Evaluator
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")

# CrossValidator
cv = CrossValidator(estimator=rf,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3)

# Train with cross-validation
cvModel = cv.fit(train_df)

# Best model
best_model = cvModel.bestModel

# Evaluate on test data
predictions = best_model.transform(test_df)
rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="r2").evaluate(predictions)

print("✅ Model Performance:")
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.2f}")

# Save performance to CSV
performance_path = "../output/section-4/model_performance.csv"
spark.createDataFrame([(rmse, r2)], ["RMSE", "R2"]) \
    .coalesce(1).write.option("header", "true").mode("overwrite").csv(performance_path)

# Prepare and save predictions
results_df = predictions.select(
    "timestamp", "region", "pm25", "prediction"
).withColumnRenamed("pm25", "actual_pm25") \
 .withColumnRenamed("prediction", "predicted_pm25")

results_df.coalesce(1).write.option("header", "true").csv("../output/section-4/predictions.csv", mode="overwrite")

print("✅ Predictions saved to: output/section-4/predictions.csv")
print("✅ Model performance saved to: output/section-4/model_performance.csv")

spark.stop()
