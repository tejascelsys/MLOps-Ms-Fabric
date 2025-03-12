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

import os

lakehouse_model_path = "/lakehouse/default/Files/sales_prediction_model"
print("Files in Lakehouse Model Directory:", os.listdir(lakehouse_model_path))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow.sklearn
import pandas as pd

# Define the model path in Fabric Lakehouse
lakehouse_model_path = "/lakehouse/default/Files/sales_prediction_model"

# Load the trained model
loaded_model = mlflow.sklearn.load_model(lakehouse_model_path)

print("Model loaded successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Load_Test_Data").getOrCreate()

# Load test data from a Delta table
lakehouse_table_path = "Tables/vehicle_population_data"
df = spark.read.format("delta").load(lakehouse_table_path)

# Convert Spark DataFrame to Pandas for model prediction
pdf = df.toPandas()

# Drop target column if it exists
X_test = pdf.drop(columns=["Electric_Vehicle_Type"], errors="ignore")

print("Test data loaded successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Make predictions
pdf["predicted_output"] = loaded_model.predict(X_test)

print("Predictions generated successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert Pandas DataFrame back to Spark DataFrame
spark_df = spark.createDataFrame(pdf)

# Define the new table name to store predictions
new_table_name = "vehicle_population_data_predictions"

# Save predictions to a new Delta table
spark_df.write.format("delta").mode("overwrite").saveAsTable(new_table_name)

print(f"Predictions saved successfully in new table: {new_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Initialize Spark session
spark = SparkSession.builder.appName("Label_Encoding").getOrCreate()

# Define the table containing predictions
predictions_table = "Tables/vehicle_population_data_predictions"  # Replace with your table name

# Load predictions from Delta table
df = spark.read.format("delta").load(predictions_table)

# Convert Spark DataFrame to Pandas for processing
pdf = df.toPandas()

print("Predictions loaded successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import joblib  # Ensure joblib is imported
import pandas as pd

# Load the saved LabelEncoders
encoder_load_path = "/lakehouse/default/Files/label_encoders.pkl"
label_encoders = joblib.load(encoder_load_path)

# Load the table containing encoded data + predictions from Fabric Lakehouse
lakehouse_table_path = "Tables/vehicle_population_data_predictions"  # Update with actual table
df = spark.read.format("delta").load(lakehouse_table_path).toPandas()

# List of categorical columns to inverse transform (excluding 'predicted_output')
categorical_columns = ["County", "City", "State", "Make", "Model", 
                       "Electric_Vehicle_Type", "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility", 
                       "Electric_Utility"]  # Do not include 'predicted_output'

# Apply inverse transformation to all categorical columns
for col in categorical_columns:
    if col in df.columns:  # Check if column exists in DataFrame
        df[col] = label_encoders[col].inverse_transform(df[col])

# Inverse transform the 'predicted_output' column
if "predicted_output" in df.columns:
    df["predicted_output"] = label_encoders["Electric_Vehicle_Type"].inverse_transform(df["predicted_output"])

print("All encoded columns (including predicted_output) have been inverse transformed!")

# Convert back to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Define the new Delta table path to store inverse transformed data
final_table_name = "vehicle_population_predictions_final"

# Save the transformed data back to Fabric Lakehouse
spark_df.write.format("delta").mode("overwrite").saveAsTable(final_table_name)

print(f" Fully inverse-transformed table saved successfully at {final_table_name}")



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
