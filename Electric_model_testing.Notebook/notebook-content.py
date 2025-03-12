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

import mlflow.sklearn

# Define model path in Fabric Lakehouse
lakehouse_model_path = "/lakehouse/default/Files/sales_prediction_model"

# Load the trained model
loaded_model = mlflow.sklearn.load_model(lakehouse_model_path)

print(" Model loaded successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import joblib

# Load the saved LabelEncoders
encoder_load_path = "/lakehouse/default/Files/label_encoders.pkl"
label_encoders = joblib.load(encoder_load_path)

print("Label Encoders loaded successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

user_input = {
    "County": "King",
    "City": "Seattle",
    "State": "WA",
    "Postal_Code":32596,
    "Model_Year": 2021,
    "Make": "VOLVO",
    "Model": "R1T",
    "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility": "Eligibility unknown as battery range has not been researched",
    "Electric_Range": 469,
    "Base_MSRP": 52989,
    "Legislative_District":35,
    "Electric_Utility": "NON WASHINGTON STATE ELECTRIC UTILITY",
    "2020_Census_Tract":12008021364
 }
# import requests
# from collections import OrderedDict

# # Flask server URL
# url = "https://00e3-2409-40f4-3084-4f20-336a-71e2-cfe6-cc44.ngrok-free.app/send-data"

# # Send POST request to Flask
# response = requests.post(url)

# if response.status_code == 200:
#     print("Data fetched successfully!")
#     user_input = response.json()
    
#     # Original data from the Flask response
#     original_data = user_input.get('original_data', {})

#     # Define the desired order
#     desired_order = [
#         'County', 'City', 'State', 'Postal_Code', 'Model_Year', 'Make', 'Model',
#         'Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility', 'Electric_Range',
#         'Base_MSRP', 'Legislative_District', 'Electric_Utility', '2020_Census_Tract'
#     ]

#     # Create a lowercase mapping of original data keys for case-insensitive matching
#     lower_case_mapping = {k.lower(): k for k in original_data}

#     # Create an OrderedDict based on the desired order using case-insensitive matching
#     sorted_data = OrderedDict(
#         (key, original_data.get(lower_case_mapping.get(key.lower()))) 
#         for key in desired_order
#     )

#     print(sorted_data)

# else:
#     print(f"Error: {response.status_code} - {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Convert user input to a DataFrame
user_df = pd.DataFrame([user_input])

# Encode categorical columns using the saved encoders
categorical_columns = ["County", "City", "State", "Make", "Model", 
                        "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility", 
                       "Electric_Utility"]

for col in categorical_columns:
    if col in user_df.columns:
        user_df[col] = label_encoders[col].transform(user_df[col])

print("User input successfully encoded!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop target column if present (not needed for prediction)
X_user = user_df

# Make the prediction
predicted_output = loaded_model.predict(X_user)

print(f"🔮 Model Prediction (Encoded): {predicted_output}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inverse transform the predicted output to original label
original_prediction = label_encoders["Electric_Vehicle_Type"].inverse_transform(predicted_output)

print(f"Final Prediction: {original_prediction[0]}")


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

# import requests
# import json

# # Flask server URL
# url = "https://00e3-2409-40f4-3084-4f20-336a-71e2-cfe6-cc44.ngrok-free.app/receive-prediction"


# prediction = str(original_prediction)
# # Prepare data for POST request
# data = {
#     'prediction': prediction
# }
# print(data)
# # Send POST request to Flask
# response = requests.post(url, json=data)

# if response.status_code == 200:
#     print(f"✅ Prediction sent successfully! Response: {response.json()}")
# else:
#     print(f"❌ Failed to send prediction. Error: {response.status_code} - {response.text}")


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
