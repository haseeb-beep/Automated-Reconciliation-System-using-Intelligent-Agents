# part4_exceptions.py
"""
Part 4 — Exception Management, Intelligent Categorization & Routing, Audit Trail (Explainable)

Outputs:
- out_dir/exceptions.csv           (list of exceptions with category + route)
- out_dir/routed_queues/<queue>.csv
- out_dir/audit.jsonl              (one JSON per decision / exception with features + rationale)

Usage:
python part4_exceptions.py --a Customer_Ledger_Entries_FULL.xlsx --b KH_Bank.xlsx --out ./out_results [--mapping mapping.xlsx]
"""

import argparse
import os
import json
from collections import defaultdict
from difflib import SequenceMatcher
from datetime import datetime
import pandas as pd
import numpy as np

# ---------------------------
# Utilities
# ---------------------------
def normalize_string(s):
    if pd.isna(s) or s is None:
        return ""
    s = str(s).lower().strip()
    # remove common company suffixes that create noise
    for token in ["ltd", "pvt", "inc", "bank", "corp", "co.", "company", "gmbh", "kft", "zrt", "korlatolt", "rt"]:
        s = s.replace(token, "")
    # keep alnum + spaces
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    return " ".join(s.split())

def str_sim(a, b):
    return SequenceMatcher(None, normalize_string(a), normalize_string(b)).ratio()

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def days_diff(a, b):
    try:
        return abs((pd.to_datetime(a) - pd.to_datetime(b)).days)
    except Exception:
        return None

# ---------------------------
# Preprocessing / column mapping
# ---------------------------
def load_and_standardize(ledger_path, bank_path):
    a = pd.read_excel(ledger_path) if ledger_path.lower().endswith((".xls", ".xlsx")) else pd.read_csv(ledger_path)
    b = pd.read_excel(bank_path) if bank_path.lower().endswith((".xls", ".xlsx")) else pd.read_csv(bank_path)

    # Ledger -> use likely fields (fall back if missing)
    ledger = a.copy()
    ledger_cols = {c.strip(): c for c in ledger.columns}
    # sensible defaults
    ledger["counterparty"] = ledger.get("Counterparty", ledger.get("Customer Name", ledger.get("Partner Bank Account No.", "")))
    ledger["amount"] = ledger.get("Amount (LCY)", ledger.get("Amount", ledger.get("Original Amount", ledger.get("Amount (LCY)", np.nan)))
                              )
    ledger["amount"] = pd.to_numeric(ledger["amount"], errors="coerce")
    # prefer Posting Date
    ledger["date"] = pd.to_datetime(ledger.get("Posting Date", ledger.get("VAT Date", ledger.get("Due Date", None))), errors="coerce")
    ledger["reference"] = ledger.get("Document No.", ledger.get("Entry No.", ledger.get("Document No", ledger.get("Document No.", ""))))
    ledger = ledger.reset_index().rename(columns={"index": "ledger_idx"})

    # Bank -> many hierarchical names in your file; use common fields found earlier
    bank = b.copy()
    # amount: try common paths used previously
    amount_cols_candidates = [
        "Statement.Entry.Amount.Value",
        "Statement.Entry.EntryDetails.TransactionDetails.AmountDetails.InstructedAmount.Amount.Value",
        "Statement.Entry.EntryDetails.TransactionDetails.AmountDetails.TransactionAmount.Amount.Value",
        "Statement.Entry.EntryDetails.TransactionDetails.AmountDetails.TransactionAmount.Amount.Value"
    ]
    found_amt = None
    for c in amount_cols_candidates:
        if c in bank.columns:
            found_amt = c
            break
    if found_amt:
        bank["amount"] = pd.to_numeric(bank[found_amt], errors="coerce")
    else:
        # try to find any column with 'amount' in name
        amt_cols = [c for c in bank.columns if 'amount' in c.lower()]
        bank["amount"] = pd.to_numeric(bank[amt_cols[0]] if amt_cols else np.nan, errors="coerce") if amt_cols else np.nan

    # date column candidates
    date_cols = ["Statement.Entry.BookingDate.Date", "Statement.Entry.ValueDate.Date", "Statement.Entry.BookingDate.Date"]
    found_date = None
    for c in date_cols:
        if c in bank.columns:
            found_date = c
            break
    if found_date:
        bank["date"] = pd.to_datetime(bank[found_date], errors="coerce", dayfirst=True)
    else:
        # try any column with 'date' in name
        dcols = [c for c in bank.columns if 'date' in c.lower()]
        bank["date"] = pd.to_datetime(bank[dcols[0]], errors="coerce", dayfirst=True) if dcols else pd.NaT

    # reference
    if "Statement.Entry.EntryReference" in bank.columns:
        bank["reference"] = bank["Statement.Entry.EntryReference"]
    elif "Statement.Entry.AccountServicerReference" in bank.columns:
        bank["reference"] = bank["Statement.Entry.AccountServicerReference"]
    else:
        # fallback to small-int index or EntryReference if present
        bank["reference"] = bank.get("Statement.Entry.EntryReference", bank.index.astype(str))

    # try debtor/creditor names
    debtor_col = "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Debtor.Name"
    creditor_col = "Statement.Entry.EntryDetails.TransactionDetails.RelatedParties.Creditor.Name"
    if debtor_col in bank.columns and creditor_col in bank.columns:
        bank["counterparty"] = bank[debtor_col].fillna("") + " " + bank[creditor_col].fillna("")
        bank["counterparty"] = bank["counterparty"].str.strip().replace("", np.nan)
        bank["counterparty"] = bank["counterparty"].fillna("")
    elif debtor_col in bank.columns:
        bank["counterparty"] = bank[debtor_col].fillna("")
    elif creditor_col in bank.columns:
        bank["counterparty"] = bank[creditor_col].fillna("")
    else:
        # fallback to 'Statement.Account.Name' or other name-like field
        name_cols = [c for c in bank.columns if any(tok in c.lower() for tok in ["name", "party", "relatedparties", "account.name"])]
        bank["counterparty"] = bank[name_cols[0]].fillna("") if name_cols else ""
    bank = bank.reset_index().rename(columns={"index": "bank_idx"})

    return ledger, bank

# ---------------------------
# Optional mapping loader
# ---------------------------
def load_mapping(mapping_path):
    if not mapping_path:
        return None
    if not os.path.exists(mapping_path):
        print(f"Mapping file {mapping_path} not found — continuing without mapping.")
        return None
    m = pd.read_excel(mapping_path) if mapping_path.lower().endswith((".xls", ".xlsx")) else pd.read_csv(mapping_path)
    # Expect columns ledger_code, bank_name OR similar — try to find candidates
    cols = [c.lower() for c in m.columns]
    ledger_col = None
    bank_col = None
    for c in m.columns:
        if 'ledger' in c.lower() or 'code' in c.lower() or 'cttp' in c.lower():
            ledger_col = c
            break
    for c in m.columns:
        if 'bank' in c.lower() or 'name' in c.lower() or 'counterparty' in c.lower():
            bank_col = c
            break
    if ledger_col is None or bank_col is None:
        print("Mapping file loaded but expected columns (ledger_code, bank_name) not found. Columns:", m.columns.tolist())
        return None
    m = m[[ledger_col, bank_col]].rename(columns={ledger_col: "ledger_code", bank_col: "bank_name"})
    # normalize bank_name for better matching
    m["bank_name_norm"] = m["bank_name"].apply(normalize_string)
    return m

# ---------------------------
# Candidate search + explainability
# ---------------------------
def find_best_candidate(a_row, bank_df, max_candidates=2000):
    """
    For a given ledger row, search bank_df for best candidate.
    Returns (best_idx, features_dict)
    """
    # Blocking heuristics: amount window, date window, partial name match
    a_amt = safe_float(a_row.get("amount"))
    a_date = a_row.get("date")
    name_a = a_row.get("counterparty") or ""
    ref_a = str(a_row.get("reference") or "")

    # build candidate mask
    mask = pd.Series([True] * len(bank_df))
    # amount-based mask: if ledger amount exists, look within broad window
    if not np.isnan(a_amt):
        # allow scalable window relative to magnitude
        window = max(50.0, abs(a_amt) * 0.05)  # 5% or 50 absolute
        mask = mask & (bank_df["amount"].notna() & (abs(bank_df["amount"] - a_amt) <= window))
    # date mask: within 14 days (broad)
    if pd.notna(a_date):
        mask = mask & (bank_df["date"].notna() & (bank_df["date"].apply(lambda d: abs((d - a_date).days) <= 14)))
    # if mask yields no rows, fallback to looser mask
    candidates = bank_df[mask]
    if len(candidates) == 0:
        # fallback: amount within 5000 OR any date OR top N by small amount diff
        if not np.isnan(a_amt):
            mask2 = bank_df["amount"].notna() & (abs(bank_df["amount"] - a_amt) <= max(5000.0, abs(a_amt) * 0.2))
            candidates = bank_df[mask2]
        if len(candidates) == 0:
            # final fallback: top K by absolute amount diff
            if not np.isnan(a_amt):
                bank_df["amt_diff_abs"] = (bank_df["amount"] - a_amt).abs()
                candidates = bank_df.nsmallest(min(max_candidates, 200), "amt_diff_abs")
            else:
                candidates = bank_df.sample(n=min(200, len(bank_df)), random_state=1)

    # compute feature scores and pick best
    best_score = -1.0
    best_idx = None
    best_feats = None
    for _, b in candidates.iterrows():
        name_sim = str_sim(name_a, b.get("counterparty", ""))
        ref_sim = str_sim(ref_a, str(b.get("reference", "")))
        amt_diff = None
        if not np.isnan(a_amt) and not np.isnan(b.get("amount")):
            amt_diff = abs(a_amt - b.get("amount"))
        date_d = days_diff(a_date, b.get("date"))
        # heuristic composite: weighted
        # If ref_sim is high, give strong boost
        score = 0.0
        score += 0.45 * name_sim
        score += 0.45 * ref_sim
        if amt_diff is not None:
            # normalized amount score
            amt_score = max(0.0, 1.0 - min(amt_diff / max(1.0, abs(a_amt) if not np.isnan(a_amt) else 1.0), 2.0))
            score += 0.05 * amt_score
        if date_d is not None:
            date_score = max(0.0, 1.0 - min(date_d / 14.0, 1.0))
            score += 0.05 * date_score

        if score > best_score:
            best_score = score
            best_idx = int(b.get("bank_idx"))
            best_feats = {
                "name_sim": round(name_sim, 4),
                "ref_sim": round(ref_sim, 4),
                "amt_diff": None if amt_diff is None else float(round(amt_diff, 2)),
                "date_diff_days": None if date_d is None else int(date_d),
                "composite_score": round(score, 4)
            }

    return best_idx, best_feats

# ---------------------------
# Exception categorization & routing
# ---------------------------
def categorize_exception(feats, a_row):
    """
    Determine category based on features:
    - NO_CANDIDATE: no candidate found at all
    - LOW_SCORE: candidate exists but composite_score too low
    - AMOUNT_MISMATCH: amt_diff too large
    - DATE_MISMATCH: date_diff too large
    - COUNTERPARTY_MISMATCH: name_sim very low
    """
    if feats is None:
        return "NO_CANDIDATE"
    score = feats.get("composite_score", 0)
    amt_diff = feats.get("amt_diff", None)
    date_diff = feats.get("date_diff_days", None)
    name_sim = feats.get("name_sim", 0)

    # thresholds (can be tuned)
    SCORE_GOOD = 0.6
    AMT_WARN = 1000.0
    DATE_WARN = 14
    NAME_LOW = 0.3

    if score < 0.25:
        return "LOW_SCORE"
    if amt_diff is not None and amt_diff > max(AMT_WARN, abs(safe_float(a_row.get("amount")))*0.1):
        return "AMOUNT_MISMATCH"
    if date_diff is not None and date_diff > DATE_WARN:
        return "DATE_MISMATCH"
    if name_sim < NAME_LOW:
        return "COUNTERPARTY_MISMATCH"
    # default fallback
    return "AMBIGUOUS"

def route_exception(category, a_row):
    """
    Simple routing rules:
    - High value exceptions -> Finance_Senior
    - Counterparty code patterns (CTTP) -> AP_Team
    - DATE_MISMATCH -> Payments_Team
    - Default -> Ops_Team
    """
    amt = safe_float(a_row.get("amount"))
    cp = str(a_row.get("counterparty") or "").upper()

    if amt and abs(amt) >= 500000:  # large value
        return "Finance_Senior"
    if "CTTP" in cp or "CTPR" in cp:  # example ledger codes
        return "AP_Team"
    if category == "DATE_MISMATCH":
        return "Payments_Team"
    if category == "AMOUNT_MISMATCH":
        return "Reconciliation_Team"
    return "Ops_Team"

# ---------------------------
# Main orchestration
# ---------------------------
def main(args):
    ledger, bank = load_and_standardize(args.a, args.b)
    mapping = load_mapping(args.mapping) if args.mapping else None

    exceptions = []
    audit_events = []
    routes = defaultdict(list)

    # Optional mapping: if provided, expand ledger rows with expected bank name for stronger matching
    if mapping is not None:
        # build dict ledger_code -> normalized bank_name
        map_dict = {}
        for _, r in mapping.iterrows():
            map_dict[str(r["ledger_code"])] = r["bank_name_norm"]
    else:
        map_dict = {}

    print(f"Ledger rows: {len(ledger)}, Bank rows: {len(bank)}")
    print("Beginning exception detection & categorization...")

    # iterate ledger rows and find best candidate
    for _, a_row in ledger.iterrows():
        # if mapping exists for this ledger reference or code, set a hint in a_row for matching
        ledger_code = str(a_row.get("counterparty") or "")
        if ledger_code in map_dict:
            # inject a stronger normalized counterparty into a temporary field to help matching
            a_row = a_row.copy()
            a_row["counterparty"] = map_dict[ledger_code]

        best_idx, feats = find_best_candidate(a_row, bank)
        category = categorize_exception(feats, a_row)
        route = route_exception(category, a_row)

        exc = {
            "ledger_idx": int(a_row.get("ledger_idx")),
            "ledger_ref": str(a_row.get("reference") or ""),
            "ledger_amount": None if pd.isna(a_row.get("amount")) else float(a_row.get("amount")),
            "ledger_date": str(a_row.get("date") if not pd.isna(a_row.get("date")) else ""),
            "ledger_counterparty": str(a_row.get("counterparty") or ""),
            "best_bank_idx": best_idx,
            "features": feats,
            "category": category,
            "route_to": route,
            "detected_ts": datetime.utcnow().isoformat()
        }
        exceptions.append(exc)
        routes[route].append(exc)

        # audit event
        audit = {
            "event": "EXCEPTION_DETECTED",
            "ledger_idx": int(a_row.get("ledger_idx")),
            "best_bank_idx": best_idx,
            "category": category,
            "features": feats,
            "route_to": route,
            "ts": datetime.utcnow().isoformat()
        }
        audit_events.append(audit)

    # write exceptions and routed queues
    os.makedirs(args.out, exist_ok=True)
    exc_df = pd.DataFrame(exceptions)
    exc_df.to_csv(os.path.join(args.out, "exceptions.csv"), index=False)

    queues_dir = os.path.join(args.out, "routed_queues")
    os.makedirs(queues_dir, exist_ok=True)
    for q, items in routes.items():
        pd.DataFrame(items).to_csv(os.path.join(queues_dir, f"{q}.csv"), index=False)

    # write audit jsonl
    with open(os.path.join(args.out, "audit.jsonl"), "w", encoding="utf-8") as f:
        for ev in audit_events:
            f.write(json.dumps(ev, default=str) + "\n")

    print(f"Done. Exceptions: {len(exceptions)}. Queues: {len(routes)}. Outputs in {args.out}")

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Part4 - Exception Management and Routing")
    p.add_argument("--a", required=True, help="Ledger file (csv/xlsx)")
    p.add_argument("--b", required=True, help="Bank file (csv/xlsx)")
    p.add_argument("--out", required=True, help="Output folder")
    p.add_argument("--mapping", required=False, help="Optional mapping file (ledger_code -> bank_name)")
    args = p.parse_args()

    main(args)
