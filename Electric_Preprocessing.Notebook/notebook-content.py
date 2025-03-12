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
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# Import necessary PySpark modules
from pyspark.sql import SparkSession

# Create or retrieve the SparkSession
spark = SparkSession.builder.appName("CSV_to_Delta").getOrCreate()

# Define paths
lakehouse_path = "Files/Electric_Vehicle_Population_Data"  # Replace with your Lakehouse file path
delta_table_path = "Tables/Electric_Vehicle_Population_Data"      # Target Delta table path in Lakehouse

# Read the CSV file into a DataFrame
csv_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(lakehouse_path)

# Clean column names
cleaned_columns = [
    col.replace(' ', '_')  # Replace spaces with underscores
       .replace('(', '')   # Remove opening parenthesis
       .replace(')', '')   # Remove closing parenthesis
       .replace('-', '_')  # Replace hyphens with underscores
       .replace('/', '_')  # Replace slashes with underscores
       .replace('.', '')   # Remove dots
       .strip()            # Strip leading/trailing spaces
    for col in csv_df.columns
]

# Apply cleaned column names to the DataFrame
csv_df = csv_df.toDF(*cleaned_columns)

# Write the cleaned DataFrame as a Delta table
csv_df.write.format("delta").mode("overwrite").save(delta_table_path)

print(f"CSV file converted and saved as Delta table at {delta_table_path}.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary modules
from pyspark.sql import SparkSession


# Create or retrieve the SparkSession
spark = SparkSession.builder.appName("Delta_to_Pandas").getOrCreate()

# Define the Delta table path
delta_table_path = "Tables/Electric_Vehicle_Population_Data"  # Replace with your Delta table path

# Load the Delta table into a PySpark DataFrame
delta_df = spark.read.format("delta").load(delta_table_path)

# Show the first few rows (optional)
#delta_df.show()

# Convert PySpark DataFrame to Pandas DataFrame
df = delta_df.toPandas()

# Display the Pandas DataFrame (optional)
print(df.head())




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Basic info
print(df.info())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df.describe())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for missing values
print(df.isnull().sum())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop rows where Vehicle_Location, Electric_Utility, or 2020_Census_Tract have missing values
df = df.dropna(subset=["Vehicle_Location", "Electric_Utility", "2020_Census_Tract"])
df['Legislative_District'] = df['Legislative_District'].fillna(0)
df["Electric_Range"].fillna(df["Electric_Range"].median(), inplace=True)
df["Base_MSRP"].fillna(df["Base_MSRP"].median(), inplace=True)
# Check for remaining missing values
print(df.isnull().sum())
print(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for missing values
print(df.isnull().sum())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import joblib
from sklearn.preprocessing import LabelEncoder

# List of categorical columns to encode
categorical_columns = ["County", "City", "State", "Make", "Model", 
                       "Electric_Vehicle_Type", "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility", 
                       "Electric_Utility"]

# Drop unwanted columns
df.drop(columns=["VIN_1_10", "DOL_Vehicle_ID", "Vehicle_Location"], inplace=True, errors="ignore")

# Initialize LabelEncoder dictionary
label_encoders = {}

# Apply Label Encoding and Save Encoders
for col in categorical_columns:
    label_encoders[col] = LabelEncoder()
    df[col] = label_encoders[col].fit_transform(df[col])  # Encode data

# Save the label encoders to a file in Fabric Lakehouse
encoder_save_path = "/lakehouse/default/Files/label_encoders.pkl"
joblib.dump(label_encoders, encoder_save_path)

print(f"Label Encoders saved successfully at {encoder_save_path}")
print(df.head())  # Show transformed data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Convert Pandas DataFrame (df) to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Define the Lakehouse table name
table_name = "Vehicle_Population_Data"

# Save DataFrame as a new Delta table in the default Lakehouse
spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Table '{table_name}' has been successfully created in the Lakehouse.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
