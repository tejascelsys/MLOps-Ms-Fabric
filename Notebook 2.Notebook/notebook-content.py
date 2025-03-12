# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d46bc1e7-cd79-4d30-95af-bdbeebeffe2e",
# META       "default_lakehouse_name": "Electric_Vehicle",
# META       "default_lakehouse_workspace_id": "4e9ac160-b418-44de-99e1-874ff3fd479e"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDeltaTable").getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("County", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal_Code", IntegerType(), True),
    StructField("Model_Year", IntegerType(), True),
    StructField("Make", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility", StringType(), True),
    StructField("Electric_Range", IntegerType(), True),
    StructField("Base_MSRP", DoubleType(), True),
    StructField("Legislative_District", IntegerType(), True),
    StructField("Electric_Utility", StringType(), True),
    StructField("2020_Census_Tract", DoubleType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the DataFrame as a managed Delta table
df.write.format("delta").saveAsTable("UserInputData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the path within the Lakehouse Files section
lakehouse_path = "Files/UserInputData"

# Write the DataFrame as an external Delta table
df.write.format("delta").option("path", lakehouse_path).saveAsTable("UserInputData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List all tables to verify
spark.sql("SHOW TABLES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Flask server URL
url = "http://localhost:5000/send-data"

# Updated data to send
# data = {
#     "County": "vivek",
#     "City": "vivek",
#     "State": "vivek",
#     "Postal_Code": 32596,
#     "Model_Year": 2021,
#     "Make": "VOLVO",
#     "Model": "R1T",
#     "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility": "Eligibility unknown as battery range has not been researched",
#     "Electric_Range": 469,
#     "Base_MSRP": 52989,
#     "Legislative_District": 35,
#     "Electric_Utility": "NON WASHINGTON STATE ELECTRIC UTILITY",
#     "2020_Census_Tract": 12008021364
# }

# Send POST request
response = requests.post(url, json=data)

if response.status_code == 200:
    print("Data fetched successfully!")
    print(response.json())
else:
    print(f"Error: {response.status_code} - {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from pyspark.sql import SparkSession

# Flask server URL
url = "https://00e3-2409-40f4-3084-4f20-336a-71e2-cfe6-cc44.ngrok-free.app/send-data"

# Send POST request to Flask
response = requests.post(url)

if response.status_code == 200:
    print("Data fetched successfully!")
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code} - {response.text}")

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("FetchDataFromFlask") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

# # Convert JSON data to DataFrame
# data = [response.json()['original_data']]  # Extract the original data
# df = spark.createDataFrame(data)

# # Write data to Lakehouse table
# #df.write.format("delta").mode("append").saveAsTable("Electric_Vehicle_Population_Data")
# df.write.format("delta") \
#     .mode("append") \
#     .option("mergeSchema", "true") \
#     .saveAsTable("Electric_Vehicle_Population_Data")


# print("Data saved to Lakehouse successfully!")


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

# CELL ********************


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

# CELL ********************


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

# CELL ********************


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
