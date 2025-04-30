"""

Muted by Eric for section 5

# SECTION 4: PM2.5 Regression Using Spark MLlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Step 0: Spark session
spark = SparkSession.builder.appName("PM2.5 Prediction").getOrCreate()

# Step 1: Load Dataset
file_path = "output/section2_step5_feature_enhanced_csv/part-00000-e8b4f274-9ba9-44a3-ba70-30c70d3ab1a3-c000.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 2: Feature Selection
selected_features = [
    "temperature", "humidity",
    "pm25_lag1", "temperature_lag1", "humidity_lag1",
    "pm25_rate_change", "temperature_rate_change", "humidity_rate_change",
    "rolling_pm25_avg"
]
data = data.select(selected_features + ["pm25"])
data = data.fillna(0, subset=selected_features + ["pm25"])


# Step 3: Assemble Features
assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
data_assembled = assembler.transform(data).select("features", "pm25")

# Step 4: Train-Test Split
train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=42)

# Fix: Cast `features` to string before saving to CSV
train_data_fixed = train_data.withColumn("features", col("features").cast("string"))
test_data_fixed = test_data.withColumn("features", col("features").cast("string"))

train_data_fixed.write.mode("overwrite").csv("output1/train_data.csv", header=True)
test_data_fixed.write.mode("overwrite").csv("output1/test_data.csv", header=True)

# Step 5: Train Models

# Random Forest
rf = RandomForestRegressor(featuresCol="features", labelCol="pm25", numTrees=50)
rf_model = rf.fit(train_data)
predictions_rf = rf_model.transform(test_data)

predictions_rf_fixed = predictions_rf.withColumn("features", col("features").cast("string"))
predictions_rf_fixed.write.mode("overwrite").csv("output1/predictions_rf.csv", header=True)

# Step 6: Evaluation
rmse_eval = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
r2_eval = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="r2")

rmse_rf = rmse_eval.evaluate(predictions_rf)
r2_rf = r2_eval.evaluate(predictions_rf)

with open("output1/initial_rf_metrics.txt", "w") as f:
    f.write(f"Initial RF RMSE: {rmse_rf}\n")
    f.write(f"Initial RF R2: {r2_rf}\n")

# Step 7: Hyperparameter Tuning
paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10]) \
    .addGrid(rf.numTrees, [20, 50]) \
    .build()

cv = CrossValidator(estimator=rf,
                    estimatorParamMaps=paramGrid,
                    evaluator=rmse_eval,
                    numFolds=3)

cv_model = cv.fit(train_data)
best_model = cv_model.bestModel
best_predictions = best_model.transform(test_data)

best_predictions_fixed = best_predictions.withColumn("features", col("features").cast("string"))
best_predictions_fixed.write.mode("overwrite").csv("output1/predictions_rf_optimized.csv", header=True)

best_rmse = rmse_eval.evaluate(best_predictions)
best_r2 = r2_eval.evaluate(best_predictions)

with open("output1/final_rf_metrics.txt", "w") as f:
    f.write(f"Final RF RMSE: {best_rmse}\n")
    f.write(f"Final RF R2: {best_r2}\n")



""" 






import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

MODEL_PATH = os.getenv("AQ_MODEL_PATH", "output/models/rf_model")

def train_and_save_model(
    source_csv: str = "output/section2_step5_feature_enhanced_csv/part-*.csv",
    model_path: str = MODEL_PATH,
):
    spark = SparkSession.builder.appName("PM2.5 Train & Save").getOrCreate()
    df = spark.read.csv(source_csv, header=True, inferSchema=True).na.fill(0)

    features = [
        "temperature", "humidity",
        "pm25_lag1", "temperature_lag1", "humidity_lag1",
        "pm25_rate_change", "temperature_rate_change", "humidity_rate_change",
        "rolling_pm25_avg"
    ]
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    data = assembler.transform(df).select("features", col("pm25").alias("label"))

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=50)
    model = rf.fit(train)

    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    model.write().overwrite().save(model_path)

    preds = model.transform(test)
    rmse = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    ).evaluate(preds)
    print(f"Saved RF model to {model_path}; test RMSE={rmse:.3f}")
    spark.stop()

def load_model(model_path: str = None) -> RandomForestRegressionModel:
    path = model_path or MODEL_PATH
    return RandomForestRegressionModel.load(path)

if __name__ == "__main__":
    train_and_save_model()
