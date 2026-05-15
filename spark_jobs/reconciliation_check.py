
import pandas as pd
import sys
import os

run_date = sys.argv[1]

# LOAD DATA
bronze = pd.read_parquet(
    f"/opt/airflow/data/bronze/Date={run_date}/transactions.parquet"
)

silver = pd.read_parquet(
    f"/opt/airflow/data/silver/Date={run_date}/transactions_cleaned.parquet"
)

# ROW COUNTS
bronze_count = len(bronze)
silver_count = len(silver)

removed_rows = bronze_count - silver_count

# CREATE REPORT
report = pd.DataFrame({
    "layer": ["bronze", "silver"],
    "row_count": [bronze_count, silver_count],
    "removed_rows": [0, removed_rows]
})

# SAVE REPORT
quality_path = (
    f"/opt/airflow/data/quality/Date={run_date}"
)

os.makedirs(quality_path, exist_ok=True)

report.to_csv(
    f"{quality_path}/reconciliation_report.csv",
    index=False
)

print(report)

# VALIDATION
if silver_count > bronze_count:

    raise Exception(
        "Silver layer has more rows than Bronze!"
    )

print("Reconciliation Check Passed")