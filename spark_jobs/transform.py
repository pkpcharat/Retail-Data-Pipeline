
import pandas as pd
import hashlib
import sys
import os

run_date = sys.argv[1]

# LOAD BRONZE DATA
df = pd.read_parquet(
    f"/opt/airflow/data/bronze/Date={run_date}/transactions.parquet"
)

# REMOVE DUPLICATES
df = df.drop_duplicates()

# REMOVE INVALID RECORDS
df = df[
    (df["Total_Items"] > 0) &
    (df["Total_Cost"] > 0)
]

# HASH CUSTOMER NAME
def hash_customer(name):

    return hashlib.sha256(
        str(name).encode()
    ).hexdigest()

df["Customer_Name_Hash"] = df[
    "Customer_Name"
].apply(hash_customer)

# DROP ORIGINAL NAME
df = df.drop(
    columns=["Customer_Name"]
)

# FEATURE ENGINEERING
df["avg_price_per_item"] = (
    df["Total_Cost"] / df["Total_Items"]
)

# OUTPUT PATH
output_path = (
    f"/opt/airflow/data/silver/Date={run_date}"
)

os.makedirs(output_path, exist_ok=True)

# SAVE SILVER
df.to_parquet(
    f"{output_path}/transactions_cleaned.parquet",
    index=False
)

print("Silver Layer Complete")