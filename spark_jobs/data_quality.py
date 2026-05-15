
import pandas as pd
import sys
import os

run_date = sys.argv[1]

# LOAD DATA
df = pd.read_parquet(
    f"/opt/airflow/data/bronze/Date={run_date}/transactions.parquet"
)

issues = []

# -----------------------
# NULL CHECKS
# -----------------------

for col in df.columns:

    null_rows = df[
        df[col].isnull()
    ]

    if len(null_rows) > 0:

        temp = null_rows.copy()

        temp["issue"] = f"NULL_{col}"

        issues.append(temp)

# -----------------------
# DUPLICATE CHECK
# -----------------------

duplicates = df[
    df.duplicated(
        subset=["Transaction_ID"]
    )
]

if len(duplicates) > 0:

    duplicates["issue"] = "DUPLICATE_TRANSACTION"

    issues.append(duplicates)

# -----------------------
# INVALID TOTAL COST
# -----------------------

invalid_cost = df[
    df["Total_Cost"] <= 0
]

if len(invalid_cost) > 0:

    invalid_cost["issue"] = "INVALID_TOTAL_COST"

    issues.append(invalid_cost)

# -----------------------
# INVALID TOTAL ITEMS
# -----------------------

invalid_items = df[
    df["Total_Items"] <= 0
]

if len(invalid_items) > 0:

    invalid_items["issue"] = "INVALID_TOTAL_ITEMS"

    issues.append(invalid_items)

# -----------------------
# INVALID PAYMENT METHOD
# -----------------------

valid_payment = [
    "Cash",
    "Credit Card",
    "Debit Card",
    "UPI"
]

invalid_payment = df[
    ~df["Payment_Method"].isin(valid_payment)
]

if len(invalid_payment) > 0:

    invalid_payment["issue"] = "INVALID_PAYMENT_METHOD"

    issues.append(invalid_payment)

# -----------------------
# SAVE REPORT
# -----------------------

quality_path = (
    f"/opt/airflow/data/quality/Date={run_date}"
)

os.makedirs(quality_path, exist_ok=True)

if len(issues) > 0:

    quality_df = pd.concat(issues)

    quality_df.to_csv(
        f"{quality_path}/data_quality_report.csv",
        index=False
    )

    print(f"{len(quality_df)} issues found")

else:

    print("No quality issues found")