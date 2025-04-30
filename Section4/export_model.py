# Section4/export_model.py  ── add THESE three lines first ────────────────
import os
os.environ["HADOOP_OPTIONAL_TOOLS"] = "true"                  # silence native-hadoop warnings
os.environ["SPARK_SECURITY_FILESYSTEM_PERMISSION_CHECK"] = "false"  # skip winutils permission step
os.environ["SPARK_LOCAL_DIRS"] = os.getcwd()                  # any writable temp dir
# ------------------------------------------------------------------------

import importlib
from pyspark.ml import PipelineModel

# 1. Import the existing training script (this runs it)
ml_mod = importlib.import_module("Section4.ml")   # DO NOT change ml.py

# 2. Grab the trained PipelineModel object
model = ml_mod.best_model

# 3. Persist it so Section-5 can load it later
MODEL_PATH = "models/best_pm25_model"
model.write().overwrite().save(MODEL_PATH)
print(f"✔  Saved model to {MODEL_PATH}")
