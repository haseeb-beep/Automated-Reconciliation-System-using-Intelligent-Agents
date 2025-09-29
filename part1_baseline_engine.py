import pandas as pd
import numpy as np
import os
import argparse
from difflib import SequenceMatcher
from datetime import datetime

# ---------------------------
# Utility Functions
# ---------------------------

def normalize_string(s):
    if pd.isna(s):
        return ""
    return str(s).strip().lower()

def compute_similarity(a_row, b_row):
    """Compute fuzzy similarity score between two rows based on counterparty and reference"""
    name_sim = SequenceMatcher(None,
                               normalize_string(a_row.get("counterparty")),
                               normalize_string(b_row.get("counterparty"))).ratio()
    ref_sim = SequenceMatcher(None,
                              normalize_string(a_row.get("reference")),
                              normalize_string(b_row.get("reference"))).ratio()
    return (name_sim + ref_sim) / 2

def reconcile(df_a, df_b, outdir, threshold=0.8, tol_amount=1.0, tol_days=2):
    matched = []
    unmatched_a = []
    unmatched_b = set(df_b.index)

    for i, a_row in df_a.iterrows():
        best_match = None
        best_score = 0

        for j, b_row in df_b.iterrows():
            # Amount tolerance
            if abs(float(a_row["amount"]) - float(b_row["amount"])) > tol_amount:
                continue

            # Date tolerance
            days_diff = abs((a_row["date"] - b_row["date"]).days)
            if days_diff > tol_days:
                continue

            sim = compute_similarity(a_row, b_row)
            if sim > best_score:
                best_score = sim
                best_match = j

        if best_match is not None and best_score >= threshold:
            matched.append({
                "ledger_index": i,
                "bank_index": best_match,
                "ledger_counterparty": a_row["counterparty"],
                "bank_counterparty": df_b.loc[best_match, "counterparty"],
                "ledger_amount": a_row["amount"],
                "bank_amount": df_b.loc[best_match, "amount"],
                "ledger_date": a_row["date"],
                "bank_date": df_b.loc[best_match, "date"],
                "similarity": best_score,
            })
            unmatched_b.discard(best_match)
        else:
            unmatched_a.append({
                "ledger_index": i,
                "ledger_counterparty": a_row["counterparty"],
                "ledger_amount": a_row["amount"],
                "ledger_date": a_row["date"],
            })

    unmatched_b_list = []
    for j in unmatched_b:
        row = df_b.loc[j]
        unmatched_b_list.append({
            "bank_index": j,
            "bank_counterparty": row["counterparty"],
            "bank_amount": row["amount"],
            "bank_date": row["date"],
        })

    # Export results
    os.makedirs(outdir, exist_ok=True)
    pd.DataFrame(matched).to_excel(os.path.join(outdir, "matched.xlsx"), index=False)
    pd.DataFrame(unmatched_a).to_excel(os.path.join(outdir, "unmatched_ledger.xlsx"), index=False)
    pd.DataFrame(unmatched_b_list).to_excel(os.path.join(outdir, "unmatched_bank.xlsx"), index=False)

    print(f"Results saved in {outdir}")


# ---------------------------
# Data Loading
# ---------------------------

def load_data(path):
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    elif path.lower().endswith(".xlsx") or path.lower().endswith(".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type")


def preprocess_data(df_a, df_b):
    # Ledger standardization
    df_a = df_a.rename(columns={
        "Counterparty": "counterparty",
        "Amount": "amount",
        "Posting Date": "date",
        "Document No.": "reference"
    })

    # Bank standardization
    df_b = df_b.rename(columns={
        "Statement.Entry.Amount.Value": "amount",
        "Statement.Entry.BookingDate.Date": "date",
        "Statement.Entry.EntryReference": "reference",
        "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Debtor.Name": "debtor_name",
        "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Creditor.Name": "creditor_name"
    })

    # Handle counterparty using Debtor OR Creditor
    if "debtor_name" in df_b.columns and "creditor_name" in df_b.columns:
        df_b["counterparty"] = df_b["debtor_name"].fillna(df_b["creditor_name"])
    elif "debtor_name" in df_b.columns:
        df_b["counterparty"] = df_b["debtor_name"]
    elif "creditor_name" in df_b.columns:
        df_b["counterparty"] = df_b["creditor_name"]
    else:
        df_b["counterparty"] = ""

    # Convert dates
    for df in [df_a, df_b]:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Drop rows missing critical fields
    df_a = df_a.dropna(subset=["amount", "date"])
    df_b = df_b.dropna(subset=["amount", "date"])

    return df_a, df_b


# ---------------------------
# Main
# ---------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", required=True, help="Path to Ledger dataset (xlsx/csv)")
    parser.add_argument("--b", required=True, help="Path to Bank dataset (xlsx/csv)")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--threshold", type=float, default=0.8, help="Similarity threshold")
    parser.add_argument("--tol_amount", type=float, default=1.0, help="Tolerance on amounts")
    parser.add_argument("--tol_days", type=int, default=2, help="Tolerance on days")
    args = parser.parse_args()

    df_a = load_data(args.a)
    df_b = load_data(args.b)
    df_a, df_b = preprocess_data(df_a, df_b)

    print("Ledger shape:", df_a.shape)
    print("Bank shape:", df_b.shape)

    reconcile(df_a, df_b, args.out, args.threshold, args.tol_amount, args.tol_days)
