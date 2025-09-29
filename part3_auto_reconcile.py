import pandas as pd
import numpy as np
import argparse
import os
from difflib import SequenceMatcher
from datetime import timedelta

# -----------------------------
# String normalization
# -----------------------------
def normalize_string(s):
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    # remove common company suffixes
    for word in ["ltd", "pvt", "inc", "bank", "corp", "co.", "company"]:
        s = s.replace(word, "")
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    return " ".join(s.split())

# -----------------------------
# Preprocessing
# -----------------------------
def preprocess_data(df_ledger, df_bank):
    # Ledger relevant columns
    ledger = df_ledger.rename(columns=str.strip)
    ledger = ledger.rename(columns={"Counterparty": "counterparty"})
    ledger["amount"] = pd.to_numeric(ledger.get("Amount", pd.Series()), errors="coerce")
    ledger["date"] = pd.to_datetime(ledger.get("Posting Date", pd.Series()), errors="coerce")
    ledger["reference"] = ledger.get("Document No.", "")

    # Bank relevant columns
    bank = df_bank.rename(columns=str.strip)
    bank["amount"] = pd.to_numeric(bank.get("Statement.Entry.Amount.Value", pd.Series()), errors="coerce")
    bank["date"] = pd.to_datetime(bank.get("Statement.Entry.BookingDate.Date", pd.Series()), errors="coerce")
    bank["reference"] = bank.get("Statement.Entry.EntryReference", "")
    # merge debtor/creditor names
    bank["counterparty"] = bank.get("Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Debtor.Name", "").fillna("") + " " + \
                           bank.get("Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Creditor.Name", "").fillna("")

    return ledger, bank

# -----------------------------
# Similarity
# -----------------------------
def compute_similarity(a_row, b_row):
    name_sim = SequenceMatcher(None,
                               normalize_string(a_row.get("counterparty")),
                               normalize_string(b_row.get("counterparty"))).ratio()
    ref_sim = SequenceMatcher(None,
                              normalize_string(a_row.get("reference")),
                              normalize_string(b_row.get("reference"))).ratio()
    return (name_sim + ref_sim) / 2

# -----------------------------
# Adaptive thresholds (looser defaults)
# -----------------------------
def adaptive_thresholds():
    return 0.6, 50.0, 5  # similarity, tol_amount, tol_days

# -----------------------------
# Reconciliation
# -----------------------------
def reconcile(df_ledger, df_bank, outdir):
    sim_thresh, tol_amount, tol_days = adaptive_thresholds()
    print(f"--- Dynamic Thresholds Selected ---\nSimilarity: {sim_thresh}, tol_amount: {tol_amount}, tol_days: {tol_days}\n")

    matched, unmatched_ledger, unmatched_bank = [], [], []
    used_bank_idx = set()

    for i, a_row in df_ledger.iterrows():
        found_match = False
        for j, b_row in df_bank.iterrows():
            if j in used_bank_idx:
                continue
            # Check amount and date tolerances
            amt_diff = abs((a_row.get("amount") or 0) - (b_row.get("amount") or 0))
            date_diff = abs((a_row.get("date") - b_row.get("date")).days) if pd.notna(a_row.get("date")) and pd.notna(b_row.get("date")) else 999
            sim = compute_similarity(a_row, b_row)

            if sim >= sim_thresh and amt_diff <= tol_amount and date_diff <= tol_days:
                matched.append({
                    "ledger_doc": a_row.get("reference"),
                    "bank_ref": b_row.get("reference"),
                    "ledger_amt": a_row.get("amount"),
                    "bank_amt": b_row.get("amount"),
                    "ledger_date": a_row.get("date"),
                    "bank_date": b_row.get("date"),
                    "ledger_party": a_row.get("counterparty"),
                    "bank_party": b_row.get("counterparty"),
                    "similarity": sim,
                    "amt_diff": amt_diff,
                    "date_diff": date_diff
                })
                used_bank_idx.add(j)
                found_match = True
                break
        if not found_match:
            unmatched_ledger.append(a_row.to_dict())

    # remaining unmatched bank rows
    for j, b_row in df_bank.iterrows():
        if j not in used_bank_idx:
            unmatched_bank.append(b_row.to_dict())

    # Export results
    os.makedirs(outdir, exist_ok=True)
    pd.DataFrame(matched).to_excel(os.path.join(outdir, "matched.xlsx"), index=False)
    pd.DataFrame(unmatched_ledger).to_excel(os.path.join(outdir, "unmatched_ledger.xlsx"), index=False)
    pd.DataFrame(unmatched_bank).to_excel(os.path.join(outdir, "unmatched_bank.xlsx"), index=False)

    # Summary
    summary = {
        "total_ledger": len(df_ledger),
        "total_bank": len(df_bank),
        "matched": len(matched),
        "unmatched_ledger": len(unmatched_ledger),
        "unmatched_bank": len(unmatched_bank),
        "match_rate": round(len(matched) / max(len(df_ledger), 1), 2)
    }
    print("--- Reconciliation Summary ---")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print(f"\nResults saved in {outdir}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", required=True, help="Ledger Excel file")
    parser.add_argument("--b", required=True, help="Bank Excel file")
    parser.add_argument("--out", required=True, help="Output folder")
    args = parser.parse_args()

    df_a = pd.read_excel(args.a)
    df_b = pd.read_excel(args.b)
    ledger, bank = preprocess_data(df_a, df_b)
    print("\n--- Ledger sample ---")
    print(ledger[["date", "amount", "reference", "counterparty"]].head(10))

    print("\n--- Bank sample ---")
    print(bank[["date", "amount", "reference", "counterparty"]].head(10))

    reconcile(ledger, bank, args.out)
