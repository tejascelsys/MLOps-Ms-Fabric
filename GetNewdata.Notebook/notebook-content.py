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

from azure.storage.blob import BlobServiceClient

# Azure Storage Details
blob_account_name = "celfabricdatalake"
blob_container_name = "mlops-poc"


# Construct the full service URL
storage_url = f"https://{blob_account_name}.blob.core.windows.net"
container_url = f"{storage_url}/{blob_container_name}"

# Connect to the Blob Service
blob_service_client = BlobServiceClient(account_url=storage_url, credential=sas_token)
container_client = blob_service_client.get_container_client(blob_container_name)

# List all blobs
blob_list = list(container_client.list_blobs())
if not blob_list:
    raise ValueError("No files found in the container.")

# Sort files by last modified date (newest first)
blob_list.sort(key=lambda x: x.last_modified, reverse=True)

# Get the latest file
latest_file = blob_list[0].name
print(f"Latest file: {latest_file}")

# Construct the WASBS path for Spark
wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{latest_file}"
spark.conf.set(f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net", sas_token)

# Read new data from latest CSV
df_new = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(wasbs_path)



# Read existing data from Fabric Lakehouse
lakehouse_path = "abfss://MlOps_Poc@onelake.dfs.fabric.microsoft.com/Electric_Vehicle.Lakehouse/Files/Electric_Vehicle_Population_Data"



# Read existing data from Fabric Lakehouse
# Read new CSV file from Azure Blob Storage
# df_new = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(wasbs_path)

# df_new.show(20)  # Print first 20 rows

# Read existing data from Fabric Lakehouse
df_existing = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(lakehouse_path)

# Perform anti-join to find new records not present in existing data
df_new_unique = df_new.join(df_existing, on="VIN (1-10)", how="left_anti")

# Count newly added records
new_count = df_new_unique.count()
print(f"Total newly inserted rows: {new_count}")

# Append only new records
df_final = df_existing.union(df_new_unique)

# Save back to Lakehouse (overwrite with updated data)
df_final.coalesce(1).write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(lakehouse_path)

print("Updated CSV file successfully saved to Fabric Lakehouse.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
