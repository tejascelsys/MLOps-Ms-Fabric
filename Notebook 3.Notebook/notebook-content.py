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
import mlflow
import pandas as pd

# Set up Spark session
spark = SparkSession.builder.appName("MLflow_Model_Comparison").getOrCreate()

# Set MLflow experiment name
experiment_name = "experiment-electric"

# Get experiment details
experiment = mlflow.get_experiment_by_name(experiment_name)
if experiment is None:
    print(f"Experiment '{experiment_name}' not found in MLflow.")
else:
    experiment_id = experiment.experiment_id

    # Fetch all runs
    runs = mlflow.search_runs(experiment_ids=[experiment_id])

    # Select relevant columns (accuracy and model name)
    results = runs[["run_id", "tags.mlflow.runName", "metrics.training_accuracy_score"]]
    results = results.rename(columns={"tags.mlflow.runName": "Model", "metrics.training_accuracy_score": "Accuracy"})

    # Filter out models with accuracy = 1.0
    results = results[results["Accuracy"] < 1.0]

    # Sort models by accuracy (descending order)
    results = results.sort_values(by="Accuracy", ascending=False)

    # Define the Lakehouse path (update with your path)
    output_dir = 'abfss://MlOps_Poc@onelake.dfs.fabric.microsoft.com/Electric_Vehicle.Lakehouse/Files'

    # Convert Pandas DataFrame to Spark DataFrame
    results_df = pd.DataFrame(results)
    spark_df = spark.createDataFrame(results_df)

    # Save as a Delta table (Lakehouse)
    spark_df.write.format("delta").mode("overwrite").save(output_dir)

    # Print top models
    print("\nModel Accuracy Comparison (Excluding Accuracy = 1.0):")
    print(results)

    # Get the best model based on accuracy
    if not results.empty:
        best_model_name = results.iloc[0]["Model"]
        best_accuracy = results.iloc[0]["Accuracy"]
        print(f"\n🎯 Best Model (Excluding Accuracy = 1.0): {best_model_name} with Accuracy: {best_accuracy}")
    else:
        print("No models with accuracy below 1.0 found.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import mlflow
import mlflow.sklearn
import shutil
import os

# Set up Spark session
spark = SparkSession.builder.appName("MLflow_Save_Model").getOrCreate()

# Set MLflow experiment name
experiment_name = "experiment-electric"

# Get experiment details
experiment = mlflow.get_experiment_by_name(experiment_name)

if experiment is None:
    print(f"Experiment '{experiment_name}' not found in MLflow.")
else:
    experiment_id = experiment.experiment_id

    # Fetch all runs
    runs = mlflow.search_runs(experiment_ids=[experiment_id])

    # Select relevant columns (run_id, model name, and accuracy)
    results = runs[["run_id", "tags.mlflow.runName", "metrics.training_accuracy_score"]]
    results = results.rename(columns={"tags.mlflow.runName": "Model", "metrics.training_accuracy_score": "Accuracy"})

    # Filter out models with accuracy = 1.0
    results = results[results["Accuracy"] < 1.0]

    if results.empty:
        print("No valid models found (all models have Accuracy = 1.0)")
    else:
        # Select the best model based on accuracy (highest accuracy < 1.0)
        best_run = results.sort_values(by="Accuracy", ascending=False).iloc[0]
        best_run_id = best_run["run_id"]

        # Define the MLflow model path
        model_uri = f"runs:/{best_run_id}/model"

        # Load the best model from MLflow
        best_model = mlflow.sklearn.load_model(model_uri)


        local_model_path = "/tmp/sales_prediction_model"

        # Remove existing directory if it exists
        if os.path.exists(local_model_path):
            shutil.rmtree(local_model_path)

        mlflow.sklearn.save_model(best_model, local_model_path)

        # Step 2: Define Fabric Lakehouse path
        lakehouse_model_path = "/lakehouse/default/Files/sales_prediction_model"

        # Step 3: Copy model files manually
        shutil.copytree(local_model_path, lakehouse_model_path, dirs_exist_ok=True)

        print(f"Best model (Accuracy = {best_run['Accuracy']}) saved at {lakehouse_model_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow.sklearn
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Save_Predictions").getOrCreate()

# Define model path in Fabric Lakehouse
lakehouse_model_path = "/lakehouse/default/Files/sales_prediction_model"

# Load model
loaded_model = mlflow.sklearn.load_model(lakehouse_model_path)

# Load input data from vehicle_population_data
lakehouse_table_path = "Tables/vehicle_population_data"
df = spark.read.format("delta").load(lakehouse_table_path)

# Convert Spark DataFrame to Pandas
pdf = df.toPandas()

# Make predictions (assuming 'X_test' is a subset of relevant columns)
# Exclude the target column and 'predicted_output' column if it exists
X_test = pdf.drop(columns=["Electric_Vehicle_Type"], errors="ignore")

# Make predictions
pdf["predicted_output"] = loaded_model.predict(X_test)

# Convert Pandas DataFrame back to Spark DataFrame
spark_df = spark.createDataFrame(pdf)

# Define new Delta table name (valid table name without slashes)
new_table_name = "vehicle_population_data_predictions"

# Save the new table with predictions
spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(new_table_name)

print(f"Predictions saved successfully in new table: {new_table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
