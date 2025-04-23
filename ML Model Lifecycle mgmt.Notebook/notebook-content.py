# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "554efbee-45aa-4956-b8ef-848d59654ea6",
# META       "default_lakehouse_name": "Electric_Vehicle",
# META       "default_lakehouse_workspace_id": "e2249e57-ea09-4468-a065-84e017543abe",
# META       "known_lakehouses": [
# META         {
# META           "id": "554efbee-45aa-4956-b8ef-848d59654ea6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# Define schema
schema = StructType([
    StructField("Dataset_Type", StringType(), True),
    StructField("Dataset_Version_Date", DateType(), True),
    StructField("Model_Version", StringType(), True),
    StructField("Model_Accuracy", FloatType(), True),
    StructField("Deployment_Status", StringType(), True),
    StructField("Production_Model_Version", StringType(), True),
    StructField("Production_Model_Accuracy", FloatType(), True),
    StructField("Real_Data_Accuracy_Validation", FloatType(), True)
])

# Create empty DataFrame with the schema
df = spark.createDataFrame([], schema)

# Save it as a Delta table in the Lakehouse
df.write.format("delta").mode("overwrite").saveAsTable("Model_Tracking")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Electric_Vehicle.model_tracking LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
