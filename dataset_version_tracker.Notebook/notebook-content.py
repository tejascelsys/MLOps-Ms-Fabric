# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Setup

# CELL ********************

from datetime import datetime
import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# Define base paths
source_path = "Files/datasets/incoming/"
base_versioned_path = "Files/datasets/"

# Define model tracking table path
model_tracking_path = "Tables/Model_Tracking"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Detect New File

# CELL ********************

# List all files in the incoming folder
files = dbutils.fs.ls(source_path)
file_names = [f.name for f in files if f.name.endswith(".csv") or f.name.endswith(".parquet")]

if not file_names:
    raise Exception("No new dataset files found.")

# Pick the first file (you can loop for all files if needed)
new_file = file_names[0]
new_file_path = source_path + new_file

print(f"New dataset found: {new_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Determine Next Dataset Version

# CELL ********************

# List version folders already present
folders = dbutils.fs.ls(base_versioned_path)
versions = [int(f.name.strip('/').replace('v', '')) for f in folders if f.name.startswith('v') and f.name.strip('/').replace('v', '').isdigit()]
next_version = max(versions) + 1 if versions else 1

# Define destination version path
version_path = f"{base_versioned_path}v{next_version}/"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Move File to Versioned Folder

# CELL ********************

# Move the file to the new version folder
dbutils.fs.mv(new_file_path, version_path + new_file)

print(f"Moved {new_file} to {version_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Log to Model_Tracking Table

# CELL ********************

# Dummy model accuracy placeholder (update during training phase)
model_accuracy = 0.0

# Create DataFrame with metadata
data = [( "Dataset",                     # Dataset Type
          datetime.now().strftime("%Y-%m-%d"),  # Dataset Version Date
          f"NA",                         # Model Version (yet to train)
          model_accuracy,               # Model Accuracy
          "No",                         # Deployment Status
          "NA",                         # Production Model Version
          0.0,                          # Production Model Accuracy
          0.0)                          # Real Data Accuracy Validation
        ]

schema = StructType([
    StructField("Dataset_Type", StringType(), True),
    StructField("Dataset_Version_Date", StringType(), True),
    StructField("Model_Version", StringType(), True),
    StructField("Model_Accuracy", FloatType(), True),
    StructField("Deployment_Status", StringType(), True),
    StructField("Production_Model_Version", StringType(), True),
    StructField("Production_Model_Accuracy", FloatType(), True),
    StructField("Real_Data_Accuracy_Validation", FloatType(), True)
])

df = spark.createDataFrame(data, schema)

# Append to the tracking table
df.write.mode("append").format("delta").save(model_tracking_path)

print("Tracking table updated.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
