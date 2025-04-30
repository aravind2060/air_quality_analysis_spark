#!/usr/bin/env python


import os
import sys
import pathlib

# 1) Put project root on PYTHONPATH
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Force Spark to use this Python interpreter
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark
SPARK_HOME = os.getenv("SPARK_HOME") or pathlib.Path(pyspark.__file__).resolve().parent
os.environ["SPARK_HOME"] = str(SPARK_HOME)

# On Windows, ensure winutils.exe
if os.name == "nt":
    HADOOP_HOME = os.getenv("HADOOP_HOME", SPARK_HOME)
    winutils = pathlib.Path(HADOOP_HOME) / "bin" / "winutils.exe"
    if not winutils.exists():
        raise RuntimeError(f"winutils.exe not found at {winutils}")
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, split, trim, to_timestamp,
    col, first, current_timestamp
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel

# your real helpers
from Section2.data_agg_transf_trend_analysis import enrich_features
from Section3.Section3_sql_exploration_and_aqi_classification import add_aqi

FEATURE_COLS = [
    "temperature", "humidity",
    "pm25_lag1", "temperature_lag1", "humidity_lag1",
    "pm25_rate_change", "temperature_rate_change", "humidity_rate_change",
    "rolling_pm25_avg"
]

def load_model():
    """
    Load the best PM2.5 RandomForest model saved by Section 4.
    """
    model_path = PROJECT_ROOT / "models" / "best_pm25_model"
    return RandomForestRegressionModel.load(str(model_path))


def build_raw_stream(spark: SparkSession):
    raw = (
        spark.readStream
             .format("socket")
             .option("host", "localhost")
             .option("port", 9999)
             .load()
    )
    return (
        raw
        .withColumn(
            "value",
            regexp_replace(
                regexp_replace(col("value"), r"[\\[\\]]", ""),
                "'", ""
            )
        )
        .withColumn("parts", split(col("value"), r",\s*"))
        .select(
            trim(col("parts")[0]).alias("location_id"),
            to_timestamp(trim(col("parts")[3]),
                         "yyyy-MM-dd'T'HH:mm:ssXXX").alias("event_time"),
            col("parts")[4].cast("double").alias("latitude"),
            col("parts")[5].cast("double").alias("longitude"),
            trim(col("parts")[6]).alias("parameter"),
            col("parts")[8].cast("double").alias("value")
        )
    )


def main():
    spark = (
        SparkSession.builder
            .appName("AQ_Pipeline_Sec5")
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.pyspark.python",        sys.executable)
            .getOrCreate()
    )

    raw_stream = build_raw_stream(spark)

    # parse JDBC url & props
    raw_jdbc = os.getenv(
        "AIRQ_JDBC",
        "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=airq"
    )
    url, param_str = raw_jdbc.split("?", 1)
    props = dict(p.split("=", 1) for p in param_str.split("&"))
    props["driver"] = "org.postgresql.Driver"

    checkpoint = str(PROJECT_ROOT / "output" / "checkpoints" / "section5")

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    rf_model  = load_model()    # <-- load RF correctly

    def foreach_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        # 1) pivot & agg into pm25, temperature, humidity
        pivoted = (
            batch_df
            .groupBy("location_id", "latitude", "longitude", "event_time")
            .pivot("parameter", ["pm25","temperature","humidity"])
            .agg(first("value"))
        )

        # 2) your Python featurization + AQI
        feat = enrich_features(pivoted)
        feat = add_aqi(feat)

        # 3) assemble feature vector
        vect = assembler.transform(feat)

        # 4) scoring + ingest_time
        scored = rf_model.transform(vect).withColumn("ingest_time", current_timestamp())

        # 5) sink to Postgres
        scored.write.jdbc(
            url=url,
            table="public.air_quality_live",
            mode="append",
            properties=props
        )

    (
        raw_stream.writeStream
          .foreachBatch(foreach_batch)
          .trigger(processingTime="10 seconds")
          .option("checkpointLocation", checkpoint)
          .outputMode("append")
          .start()
          .awaitTermination()
    )


if __name__ == "__main__":
    main()
