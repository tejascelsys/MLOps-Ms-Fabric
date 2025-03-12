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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Welcome to your new notebook
# Type here in the cell editor to add code!
# Azure storage access info for open dataset diabetes
blob_account_name = "celfabricdatalake"
blob_container_name = "mlops-poc"
blob_relative_path = "Electric_Vehicle_Population_Data.csv"
blob_sas_token = r"" # Blank since container is Anonymous access
    
# Set Spark config to access  blob storage
# Set Spark config to access blob storage
wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
spark.conf.set(f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net", blob_sas_token)

print("Remote blob path: " + wasbs_path)

# Read CSV using Spark
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(wasbs_path)

df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the Lakehouse file path for CSV
lakehouse_path = "abfss://MlOps_Poc@onelake.dfs.fabric.microsoft.com/Electric_Vehicle.Lakehouse/Files/Electric_Vehicle_Population_Data"

# Save DataFrame as CSV
df.coalesce(1).write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(lakehouse_path)

print(f"CSV file successfully saved to {lakehouse_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
