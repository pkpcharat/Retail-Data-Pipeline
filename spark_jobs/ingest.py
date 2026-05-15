
import pandas as pd
import sys
import os

# GET AIRFLOW DATE
run_date = sys.argv[1]

print(f"Processing date: {run_date}")

# LOAD CSV
df = pd.read_csv(
    "/opt/airflow/data/bronze/Retail_Transactions_Dataset.csv"
)

# CONVERT DATE
df["Date"] = pd.to_datetime(
    df["Date"],
    dayfirst=True
).dt.date

# FILTER DATA BY DATE
df = df[
    df["Date"].astype(str) == run_date
]

# OUTPUT PATH
output_path = (
    f"/opt/airflow/data/bronze/Date={run_date}"
)

os.makedirs(output_path, exist_ok=True)

# SAVE PARQUET
df.to_parquet(
    f"{output_path}/transactions.parquet",
    index=False
)

print("Bronze Layer Complete")