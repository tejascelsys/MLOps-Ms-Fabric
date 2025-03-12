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

import pandas as pd


df = spark.read.format("delta").load("Tables/vehicle_population_data").toPandas()
print(df.head(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Features (input variables) - Drop the target column
X = df.drop(columns=['Electric_Vehicle_Type'])

# Target variable (output)
y = df['Electric_Vehicle_Type']

# Check if any missing values exist
print(X.isnull().sum())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Training size: {len(X_train)}, Testing size: {len(X_test)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
experiment_name = "experiment-electric"
mlflow.set_experiment(experiment_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import RandomForestClassifier

# Create and train the model
RandomForestClassifier = RandomForestClassifier(n_estimators=100, random_state=42)
RandomForestClassifier.fit(X_train, y_train)

# Make predictions
y_pred = RandomForestClassifier.predict(X_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.linear_model import LogisticRegression

LogisticRegression = LogisticRegression()
LogisticRegression.fit(X_train, y_train)

y_pred = LogisticRegression.predict(X_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.tree import DecisionTreeClassifier

DecisionTreeClassifier = DecisionTreeClassifier(max_depth=5, random_state=42)
DecisionTreeClassifier.fit(X_train, y_train)

y_pred = DecisionTreeClassifier.predict(X_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# We will predict Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility, which is a binary classification problem (1 = Eligible, 0 = Not Eligible).

# CELL ********************

from sklearn.neighbors import KNeighborsClassifier

# Create and train the KNN model
KNeighborsClassifier = KNeighborsClassifier(n_neighbors=5)  # 'n_neighbors' is the number of nearest neighbors to consider
KNeighborsClassifier.fit(X_train, y_train)

# Make predictions
y_pred = KNeighborsClassifier.predict(X_test)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import GradientBoostingClassifier

GradientBoostingClassifier = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42)
GradientBoostingClassifier.fit(X_train, y_train)

y_pred = GradientBoostingClassifier.predict(X_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from xgboost import XGBClassifier

XGBClassifier = XGBClassifier(n_estimators=100, learning_rate=0.1, use_label_encoder=False, eval_metric='logloss')
XGBClassifier.fit(X_train, y_train)

y_pred = XGBClassifier.predict(X_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
