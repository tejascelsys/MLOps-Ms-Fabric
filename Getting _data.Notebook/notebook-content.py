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

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient 
import pandas as pd
from io import StringIO
# Replace with your actual values
STORAGE_ACCOUNT_NAME = "dataikudss001"
SAS_TOKEN = "sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-03-24T17:41:04Z&st=2025-03-24T09:41:04Z&spr=https&sig=lk9RgVOd8R%2BP3ZHFU6LwEBwuKxgae8YF0COoMszq9tc%3D"
CONTAINER_NAME = "dataiku"
BLOB_NAME = "orders.csv"

# Construct the blob URL with the SAS token
blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{BLOB_NAME}?{SAS_TOKEN}"

# Initialize BlobClient using the blob URL
blob_client = BlobClient.from_blob_url(blob_url)

# Download blob content as bytes
downloaded_blob = blob_client.download_blob().readall()

# Convert bytes to StringIO for pandas
csv_data = StringIO(downloaded_blob.decode("utf-8"))

# Read CSV into pandas DataFrame
df = pd.read_csv(csv_data)

# Display DataFrame
print(df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
