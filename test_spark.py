from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()

df = spark.createDataFrame([(1, "Air Quality"), (2, "Forecasting")], ["id", "project"])
df.show()

spark.stop()

