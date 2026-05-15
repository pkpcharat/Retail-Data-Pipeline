
import pandas as pd
import sys
import os

run_date = sys.argv[1]

# LOAD SILVER DATA
df = pd.read_parquet(
    f"/opt/airflow/data/silver/Date={run_date}/transactions_cleaned.parquet"
)

# AGGREGATION
sales_summary = (
    df.groupby("Product")["Total_Cost"]
    .sum()
    .reset_index()
)

sales_summary.columns = [
    "Product",
    "Revenue"
]

# OUTPUT PATH
output_path = (
    f"/opt/airflow/data/gold/Date={run_date}"
)

os.makedirs(output_path, exist_ok=True)

# SAVE GOLD
sales_summary.to_parquet(
    f"{output_path}/sales_summary.parquet",
    index=False
)

print("Gold Layer Complete")