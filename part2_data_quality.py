import pandas as pd
import numpy as np
import argparse
import os
import json

def load_data(path):
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    elif path.lower().endswith(".xlsx") or path.lower().endswith(".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type")

def compute_data_quality(df, name="dataset"):
    metrics = {}

    # Basic size
    metrics["rows"] = len(df)
    metrics["columns"] = len(df.columns)

    # Missing values
    metrics["missing_values"] = int(df.isna().sum().sum())
    metrics["missing_ratio"] = float(df.isna().sum().sum() / (df.size + 1e-9))

    # Duplicates
    metrics["duplicate_rows"] = int(df.duplicated().sum())

    # Date range check (if "date" column exists)
    if "date" in df.columns:
        metrics["min_date"] = str(df["date"].min())
        metrics["max_date"] = str(df["date"].max())
    else:
        metrics["min_date"] = None
        metrics["max_date"] = None

    # Amount distribution (if "amount" column exists)
    if "amount" in df.columns:
        metrics["amount_min"] = float(df["amount"].min())
        metrics["amount_max"] = float(df["amount"].max())
        metrics["amount_mean"] = float(df["amount"].mean())
        metrics["amount_std"] = float(df["amount"].std())
    else:
        metrics["amount_min"] = metrics["amount_max"] = None

    return {name: metrics}

def adjust_thresholds(dq_metrics, base_threshold=0.8, base_tol_amount=1.0, base_tol_days=2):
    """Dynamic threshold tuning based on data quality"""

    # Start with base params
    threshold = base_threshold
    tol_amount = base_tol_amount
    tol_days = base_tol_days

    missing_ratio = dq_metrics.get("missing_ratio", 0)
    duplicate_rows = dq_metrics.get("duplicate_rows", 0)
    rows = dq_metrics.get("rows", 1)

    # If high missing values, relax threshold slightly
    if missing_ratio > 0.1:
        threshold -= 0.1

    # If lots of duplicates, relax more
    if duplicate_rows > 0.05 * rows:
        threshold -= 0.05

    # If dataset is small, be stricter
    if rows < 100:
        threshold += 0.05

    # Clamp values
    threshold = min(max(threshold, 0.5), 0.95)

    return threshold, tol_amount, tol_days


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", required=True, help="Ledger file (xlsx/csv)")
    parser.add_argument("--b", required=True, help="Bank file (xlsx/csv)")
    parser.add_argument("--out", required=True, help="Output directory")
    args = parser.parse_args()

    df_a = load_data(args.a)
    df_b = load_data(args.b)

    # Data quality metrics
    dq_a = compute_data_quality(df_a, "ledger")
    dq_b = compute_data_quality(df_b, "bank")

    all_metrics = {**dq_a, **dq_b}

    # Save metrics
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "dq-metrics.json"), "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)

    print("Data Quality Metrics saved to dq-metrics.json")

    # Suggest thresholds
    ledger_thresholds = adjust_thresholds(dq_a["ledger"])
    bank_thresholds = adjust_thresholds(dq_b["bank"])

    print("\n--- Suggested Parameters ---")
    print(f"Ledger thresholds: similarity={ledger_thresholds[0]:.2f}, "
          f"tol_amount={ledger_thresholds[1]}, tol_days={ledger_thresholds[2]}")
    print(f"Bank thresholds: similarity={bank_thresholds[0]:.2f}, "
          f"tol_amount={bank_thresholds[1]}, tol_days={bank_thresholds[2]}")
